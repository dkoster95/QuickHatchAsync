//
//  TaskCoordinatorTests.swift
//  Countries
//
//  Created by Daniel Koster on 6/8/26.
//

import Testing
import Foundation
import os
import QuickHatchAsync

@Suite("Task Coalescer Concurrency & Eviction Tests")
struct TaskCoalescerTests {
    
    @Test("Verifies simultaneous callers reuse task, and late callers get a fresh request after eviction")
    func testConcurrentExecutionAndEvictionLifecycle() async throws {
        let coordinator = TaskCoalescer.shared
        let requestID = "test_country_fetch"
        
        // Track how many times the actual network block is executed
        let executionCount = OSAllocatedUnfairLock(initialState: 0)
        
        // 1. Simulating Caller 1 and Caller 2 hitting the manager at the exact same time (0ms)
        // They should share the exact same background Task.
        async let call1 = coordinator.execute(id: requestID, evictionTimeout: .seconds(1)) {
            executionCount.withLock { $0 += 1 }
            try await Task.sleep(for: .seconds(2)) // Simulates a slow 2-second network call
            return "Uruguay"
        }
        
        async let call2 = coordinator.execute(id: requestID, evictionTimeout: .seconds(1)) {
            executionCount.withLock { $0 += 1 }
            try await Task.sleep(for: .seconds(2))
            return "Uruguay"
        }
        
        // Await the first batch of simultaneous callers
        let result1 = try await call1
        let result2 = try await call2
        
        // Verify they both succeeded and shared the exact same execution block
        #expect(result1 == "Uruguay")
        #expect(result2 == "Uruguay")
        #expect(executionCount.withLock { $0 } == 1) // Crucial: Only 1 network call was made!
        
        // 2. Wait out the 1-second eviction timeout window completely
        try await Task.sleep(for: .seconds(1.5))
        
        // 3. Simulating Caller 3 hitting the manager at second 3.5
        // Since the eviction timeout was 1 second, the old key must be gone.
        // Caller 3 should transparently trigger a brand-new network request.
        let result3 = try await coordinator.execute(id: requestID, evictionTimeout: .seconds(1)) {
            executionCount.withLock { $0 += 1 }
            return "Canada"
        }
        
        // Verify Caller 3 generated a completely new isolated structured task
        #expect(result3 == "Canada")
        #expect(executionCount.withLock { $0 } == 2) // Crucial: A second fresh network call was spawned!
    }
}
