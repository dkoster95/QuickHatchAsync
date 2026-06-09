//
//  TaskCoordinator.swift
//  Countries
//
//  Created by Daniel Koster on 6/8/26.
//

import Foundation
import os

/// A thread-safe request deduplicator that coalesces simultaneous asynchronous requests,
/// tracks reference counts for safe cancellation, and evicts stalled tasks after a timeout.
public final class TaskCoalescer: TaskCoalescing, @unchecked Sendable {
    
    public static let shared = TaskCoalescer()
    
    // 1. Thread-safe internal storage record.
    // Uses its own lock to guarantee atomic updates to the reference counter.
    private final class RequestStorage<Value: Sendable>: @unchecked Sendable {
        let task: Task<Value, Error>
        private let lock = OSAllocatedUnfairLock(initialState: 0)
        
        var referenceCount: Int {
            lock.withLock { $0 }
        }
        
        init(task: Task<Value, Error>, initialCount: Int) {
            self.task = task
            self.lock.withLock { $0 = initialCount }
        }
        
        func increment() {
            lock.withLock { $0 += 1 }
        }
        
        func decrement() -> Int {
            lock.withLock {
                $0 -= 1
                return $0
            }
        }
    }
    
    // Main lock protecting the dictionary structure
    private let lock = OSAllocatedUnfairLock(initialState: [String: any Sendable]())
    
    private init() {}
    
    public func execute<Value: Sendable>(
        id: String,
        evictionTimeout: Duration = .seconds(30),
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        
        let storage: RequestStorage<Value>
        
        // --- Lock Context 1: Find or Register Task ---
        let registration = lock.withLock { state -> (RequestStorage<Value>, Bool) in
            if let existingAny = state[id],
               let existingStorage = existingAny as? RequestStorage<Value> {
                existingStorage.increment()
                return (existingStorage, false)
            } else {
                // To safely pass a reference into the Task without a data race,
                // we wrap it in a Sendable box that allows the deferred block to evaluate it later.
                let taskBox = OSAllocatedUnfairLock<Task<Value, Error>?>(initialState: nil)
                
                let newTask = Task {
                    defer {
                        // Cleans the dictionary entry upon natural completion
                        self.lock.withLock { state in
                            if let currentAny = state[id],
                               let currentStorage = currentAny as? RequestStorage<Value>,
                               currentStorage.task == taskBox.withLock({ $0 }) {
                                state.removeValue(forKey: id)
                            }
                        }
                    }
                    
                    return try await withThrowingTaskGroup(of: Value.self) { group in
                        group.addTask {
                            try await operation()
                        }
                        
                        group.addTask {
                            try await Task.sleep(for: evictionTimeout)
                            
                            // TIMEOUT HIT: Safely clear out the entry
                            self.lock.withLock { state in
                                if let currentAny = state[id],
                                   let currentStorage = currentAny as? RequestStorage<Value>,
                                   currentStorage.task == taskBox.withLock({ $0 }) {
                                    state.removeValue(forKey: id)
                                }
                            }
                            
                            try await Task.sleep(for: .seconds(999_999))
                            throw TaskError.timeout
                        }
                        
                        let result = try await group.next()!
                        group.cancelAll()
                        return result
                    }
                }
                
                taskBox.withLock { $0 = newTask }
                let newStorage = RequestStorage(task: newTask, initialCount: 1)
                state[id] = newStorage
                return (newStorage, true)
            }
        }
        
        storage = registration.0
        
        // --- 2. Structured Execution & Cancellation Handler ---
        return try await withTaskCancellationHandler {
            try await storage.task.value
        } onCancel: {
            // Synchronously decrement and check the count safely
            let remainingCount = storage.decrement()
            
            if remainingCount <= 0 {
                // Last caller cancelled, kill the underlying task immediately
                storage.task.cancel()
                
                // Scrub from the global layout if it matches
                self.lock.withLock { state in
                    if let currentAny = state[id],
                       let currentStorage = currentAny as? RequestStorage<Value>,
                       currentStorage.task == storage.task {
                        state.removeValue(forKey: id)
                    }
                }
            }
        }
    }
}

