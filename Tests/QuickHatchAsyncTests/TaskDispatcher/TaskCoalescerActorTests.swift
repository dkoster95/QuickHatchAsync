import Testing
import Foundation
import os
@testable import QuickHatchAsync // Adjust this to match your module name

@Suite("TaskCoalescerActor Production Tests")
struct TaskCoalescerActorTests {
    
    // MARK: - 1. Basic Coalescing Test
    @Test("Verify that concurrent duplicate calls are deduplicated into a single operation")
    func testRequestCoalescing() async throws {
        let coalescer = TaskCoalescerActor()
        let executionCounter = OSAllocatedUnfairLock(initialState: 0)
        
        let concurrencyCount = 5
        // FIXED: Replaced DispatchSemaphore with a modern, non-blocking AsyncGate
        let gate = AsyncGate()
        var tasks: [Task<String, Error>] = []
        
        for _ in 1...concurrencyCount {
            let task = Task {
                await gate.wait() // Elegant, non-blocking suspension point
                return try await coalescer.execute(id: "user_profile_42") {
                    executionCounter.withLock { $0 += 1 }
                    try await Task.sleep(for: .milliseconds(50))
                    return "MockData"
                }
            }
            tasks.append(task)
        }
        
        // Lower the gate for all suspended tasks simultaneously
        await gate.open()
        
        var results: [String] = []
        for task in tasks {
            let value = try await task.value
            results.append(value)
        }
        
        #expect(results.count == concurrencyCount)
        #expect(results.allSatisfy { $0 == "MockData" })
        // Crucial check: The underlying network/DB block ran EXACTLY once
        #expect(executionCounter.withLock({ $0 }) == 1)
    }
    
    // MARK: - 2. Independent Execution Test
    @Test("Verify that requests with distinct IDs run entirely independently")
    func testDistinctRequestsDoNotCoalesce() async throws {
        let coalescer = TaskCoalescerActor()
        let executionCounter = OSAllocatedUnfairLock(initialState: 0)
        
        let concurrencyCount = 3
        let gate = AsyncGate()
        var tasks: [Task<String, Error>] = []
        
        for i in 1...concurrencyCount {
            let task = Task {
                await gate.wait()
                return try await coalescer.execute(id: "id_\(i)") {
                    executionCounter.withLock { $0 += 1 }
                    return "Data"
                }
            }
            tasks.append(task)
        }
        
        await gate.open()
        
        for task in tasks {
            _ = try await task.value
        }
        
        #expect(executionCounter.withLock({ $0 }) == concurrencyCount)
    }
    
    // MARK: - 3. Reference Counted Partial Cancellation Test
    @Test("Verify that canceling 1 caller leaves the pipeline running for remaining active callers")
    func testPartialCancellationLeavesTaskRunningForOthers() async throws {
        let coalescer = TaskCoalescerActor()
        let taskInitiated = AsyncExpectation()
        let caller2Gate = AsyncGate() // FIXED: Use a gate inside the test to control caller2
        
        let caller1 = Task {
            try await coalescer.execute(id: "shared_stream") {
                await taskInitiated.fulfill() // Signal that the background task is running
                try await Task.sleep(for: .milliseconds(500)) // Give it a long shelf life
                return "SuccessResult"
            }
        }
        
        // 1. Wait until the core background task is actively running
        await taskInitiated.wait()
        
        let caller2 = Task {
            let result = try await coalescer.execute(id: "shared_stream") {
                return "Fallback"
            }
            // Force caller2 to pause right here after receiving its value
            await caller2Gate.wait()
            return result
        }
        
        // 2. FIXED: Give the background concurrent thread pool a guaranteed
        // timeline window to bind the cancellation handlers cleanly.
        try await Task.sleep(for: .milliseconds(100))
        
        // 3. Cancel Caller 1 explicitly mid-flight.
        // The registration state is fully settled. Caller 1 detaches safely.
        caller1.cancel()
        
        // Verify Caller 1 catches its expected cancellation safely
        let result1 = await caller1.result
        switch result1 {
        case .failure(let error):
            #expect(error is CancellationError)
        case .success:
            Issue.record("Caller 1 should have caught a cancellation error.")
        }
        
        // 4. Open the gate to let caller2 proceed and exit its thread frame
        await caller2Gate.open()
        
        // Caller 2 must complete successfully with the single shared execution stream data
        let result2 = try await caller2.value
        #expect(result2 == "SuccessResult")
    }

    // MARK: - 4. Total Cancellation Eviction Test
    @Test("Verify that canceling ALL active callers successfully aborts the root background task")
    func testTotalCancellationAbortsRootTask() async throws {
        let coalescer = TaskCoalescerActor()
        let taskWasCancelled = OSAllocatedUnfairLock(initialState: false)
        let checkpoint = AsyncExpectation()
        
        let caller1 = Task {
            try await coalescer.execute(id: "doomed_task") {
                await checkpoint.fulfill()
                do {
                    try await Task.sleep(for: .seconds(5))
                    return "Completed"
                } catch is CancellationError {
                    taskWasCancelled.withLock { $0 = true }
                    throw CancellationError()
                }
            }
        }
        
        await checkpoint.wait()
        
        // Abort the last remaining active listener
        caller1.cancel()
        _ = await caller1.result
        
        // Brief pause to allow the detached cooperative task cascade to deliver the signal
        try await Task.sleep(for: .milliseconds(50))
        
        #expect(taskWasCancelled.withLock({ $0 }) == true)
    }

    // MARK: - 5. Stress Testing Identity Verification Swaps
    @Test("High Concurrency Stress Test: Verify zero deadlocks or state corruption under 1,000 parallel operations")
    func testHighConcurrencyStressSimulation() async throws {
        let coalescer = TaskCoalescerActor()
        let totalDispatches = 1000
        let gate = AsyncGate()
        var tasks: [Task<Void, Error>] = []
        
        for i in 0..<totalDispatches {
            let assignedId = "stress_id_\(i % 10)"
            
            let task = Task {
                await gate.wait()
                let randomSleep = UInt64.random(in: 10_000...50_000)
                try await Task.sleep(nanoseconds: randomSleep)
                
                let data = try await coalescer.execute(id: assignedId) {
                    return "Value_\(assignedId)"
                }
                #expect(data == "Value_\(assignedId)")
            }
            tasks.append(task)
        }
        
        await gate.open()
        
        for task in tasks {
            _ = try await task.value
        }
    }
}

// MARK: - 🛠️ Swift 6 Compliant Non-Blocking Testing Primitives

/// A thread-safe, non-blocking synchronization barrier for concurrent test synchronization
actor AsyncGate {
    private var isOpened = false
    private var continuations: [CheckedContinuation<Void, Never>] = []
    
    func wait() async {
        if isOpened { return }
        await withCheckedContinuation { continuations.append($0) }
    }
    
    func open() {
        isOpened = true
        continuations.forEach { $0.resume() }
        continuations.removeAll()
    }
}

/// A thread-safe, non-blocking async fulfillment expectation helper
actor AsyncExpectation {
    private var isFulfilled = false
    private var continuations: [CheckedContinuation<Void, Never>] = []
    
    func fulfill() {
        guard !isFulfilled else { return }
        isFulfilled = true
        continuations.forEach { $0.resume() }
        continuations.removeAll()
    }
    
    func wait() async {
        if isFulfilled { return }
        await withCheckedContinuation { continuations.append($0) }
    }
}
