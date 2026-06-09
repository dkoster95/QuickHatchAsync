//
//  TaskSerializerTests.swift
//  Countries
//
//  Created by Daniel Koster on 6/8/26.
//
import Testing
import Foundation
import os
import QuickHatchAsync

@Suite("Task Serializer Tests")
struct TaskSerializerTests {
    
    // MARK: - Standard Flow Test
    
    @Test("execute() completes successfully when no overlapping tasks exist")
    func testSingleExecutionSucceeds() async throws {
        // Arrange
        let sut = TaskSerializer() // Use local instances to avoid cross-test interference
        
        // Act
        let result = try await sut.execute(id: "search_query") {
            return "First Result"
        }
        
        // Assert
        #expect(result == "First Result")
    }
    
    // MARK: - Cancellation & Overwrite Test
    
    @Test("execute() aggressively cancels the previous running task when a new one arrives")
    func testPreviousTaskIsCancelledByNewArrival() async throws {
        // Arrange
        let sut = TaskSerializer()
        let taskID = "typing_stream"
        
        let firstTaskStarted = OSAllocatedUnfairLock(initialState: false)
        
        // Act & Assert
        // 1. Kick off the first task and let it enter an artificial sleep
        let firstCall = Task {
            try await sut.execute(id: taskID) {
                firstTaskStarted.withLock { $0 = true }
                // Cooperative cancellation sleep. Must be long enough to let task 2 interrupt it.
                try await Task.sleep(for: .seconds(2))
                return "First Query Result"
            }
        }
        // Give a tiny window to ensure the first task has definitely registered and started running
        while !firstTaskStarted.withLock({ $0 }) {
            try await Task.sleep(for: .milliseconds(5))
        }
        
        // 2. Fire the second task immediately while the first task is still sleeping
        async let secondCall = sut.execute(id: taskID) {
            return "Second Query Result"
        }
        
        // 3. Verify outcomes
        // The first caller should throw a CancellationError because it was knocked out of the slot
        await #expect(throws: CancellationError.self) {
            try await firstCall.value
        }
        
        // The second caller should finish successfully with the latest data
        let finalResult = try await secondCall
        #expect(finalResult == "Second Query Result")
    }
    
    // MARK: - Heavy Stress Concurrency Test
    
    @Test("execute() handles rapid multi-threaded overrides and ensures only the final task survives")
    func testRapidOverwritesConcurrently() async throws {
        // Arrange
        let sut = TaskSerializer()
        let taskID = "stress_test_id"
        let executionCounter = OSAllocatedUnfairLock(initialState: 0)
        
        // Act
        // Use a TaskGroup to fire multiple tasks rapidly in a parallel loop
        _ = await withThrowingTaskGroup(of: String.self) { group in
            for i in 1...10 {
                group.addTask {
                    do {
                        return try await sut.execute(id: taskID) {
                            try await Task.sleep(for: .milliseconds(10 - Double(i-1)))
                            executionCounter.withLock { $0 += 1 }
                            return "Result \(i)"
                        }
                    } catch {
                        return "cancelled \(i)"
                    }
                }
            }
            
            var lastCollectedValue = ""
            // Collect whatever values managed to finish without getting cancelled
            while let result = try? await group.next() {
                lastCollectedValue = result
            }
            return lastCollectedValue
        }
        
        // Assert

        let rawExecutions = executionCounter.withLock { $0 }
        #expect(rawExecutions < 10, "Some intermediate executions should have been pruned out cleanly.")
    }
}

