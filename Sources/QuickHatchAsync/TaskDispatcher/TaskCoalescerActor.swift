//
//  TaskCoalescerActor.swift
//  Countries
//
//  Created by Daniel Koster on 6/10/26.
//

import Foundation
import os

/// A thread-safe request deduplicator using an Actor boundary.
/// Uses decoupled detached background workers to avoid re-entrancy deadlocks.
public actor TaskCoalescerActor: TaskCoalescing {
    private var tasks: [String: any Sendable] = [:]
    
    public init() {}
    
    public func execute<Value: Sendable>(
        id: String,
        evictionTimeout: Duration = .seconds(30),
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        
        let taskRegistration: RequestStorage<Value>
        
        // --- 1. Isolated Actor Context: Find or Register Task ---
        if let taskStorage = tasks[id],
           let taskFromStorage = taskStorage as? RequestStorage<Value> {
            taskRegistration = taskFromStorage
            taskRegistration.increment()
        } else {
            // Using Task.detached completely decouples the operational execution
            // from the actor's queue, making re-entrancy deadlocks impossible.
            let task = Task.detached {
                return try await operation()
            }
            
            taskRegistration = RequestStorage(task: task, initialCount: 1)
            tasks[id] = taskRegistration
        }
        
        // --- 2. Symmetrical Reference Tracking & Eviction ---
        // Attaching this single structured 'defer' directly to the caller's stack frame
        // guarantees that the tracking reference decays back to zero perfectly on any exit path.
        defer {
            handlePostAwaitEviction(id: id, record: taskRegistration)
        }
        
        // --- 3. Structured Execution Track ---
        return try await executeWithCancellationTracking(registration: taskRegistration)
    }
    
    // --- Private Helper Methods ---
    
    /// Encapsulates the withTaskCancellationHandler routing bridge logic
    private func executeWithCancellationTracking<Value: Sendable>(
        registration: RequestStorage<Value>
    ) async throws -> Value {
        return try await withTaskCancellationHandler {
            try await registration.task.value
        } onCancel: { [registration] in
            if registration.referenceCount <= 1 {
                registration.task.cancel()
            }
        }
    }
    
    /// Evaluates reference counters and evicts the entry from the registry map safely
    private func handlePostAwaitEviction<Value: Sendable>(
        id: String,
        record: RequestStorage<Value>
    ) {
        let remaining = record.decrement()
        if remaining <= 0 {
            // Pointer comparison (===) prevents wiping out a brand new task
            // that might have taken over the dictionary ID during our await gap.
            if let currentAny = tasks[id],
               let currentStorage = currentAny as? RequestStorage<Value>,
               currentStorage === record {
                tasks.removeValue(forKey: id)
            }
        }
    }
}
