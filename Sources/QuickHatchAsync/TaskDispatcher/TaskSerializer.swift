//
//  TaskSerializer.swift
//  Countries
//
//  Created by Daniel Koster on 6/8/26.
//
import Foundation
import os

/// A thread-safe utility that ensures only the LATEST asynchronous operation for a given ID runs.
/// If a new request arrives with the same ID, any active in-flight task is forcefully cancelled
/// and replaced instantly.
public final class TaskSerializer: TaskSerializing, @unchecked Sendable {
    
    public static let shared = TaskSerializer()
    
    private final class ActiveTaskRecord<Value: Sendable>: @unchecked Sendable {
        let task: Task<Value, Error>
        
        init(task: Task<Value, Error>) {
            self.task = task
        }
    }
    
    private let lock = OSAllocatedUnfairLock(initialState: [String: any Sendable]())
    
    // Made public for unit testing insulation to avoid global state pollution
    public init() {}
    
    /// Executes an operation, aggressively cancelling any previous unfinished operation sharing the same ID.
    @discardableResult
    public func execute<Value: Sendable>(
        id: String,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        // --- 1. Lock Context: Evict and Cancel the Stale Operation ---
        let record: ActiveTaskRecord<Value> = lock.withLock { state in
            if let existingAny = state[id],
               let staleRecord = existingAny as? ActiveTaskRecord<Value> {
                staleRecord.task.cancel()
            }
            
            let identityBox = OSAllocatedUnfairLock<Task<Value, Error>?>(initialState: nil)
            
            let newTask = Task {
                defer {
                    self.lock.withLock { state in
                        if let currentAny = state[id],
                           let currentRecord = currentAny as? ActiveTaskRecord<Value>,
                           currentRecord.task == identityBox.withLock({ $0 }) {
                            state.removeValue(forKey: id)
                        }
                    }
                }
                return try await operation()
            }
            
            identityBox.withLock { $0 = newTask }
            let newRecord = ActiveTaskRecord(task: newTask)
            state[id] = newRecord
            return newRecord
        }
        
        // --- 2. Structured Execution & Cancellation Forwarding ---
        return try await withTaskCancellationHandler {
            try await record.task.value
        } onCancel: {
            self.lock.withLock { state in
                if let currentAny = state[id],
                   let currentRecord = currentAny as? ActiveTaskRecord<Value>,
                   currentRecord.task == record.task {
                    record.task.cancel()
                    state.removeValue(forKey: id)
                }
            }
        }
    }
}
