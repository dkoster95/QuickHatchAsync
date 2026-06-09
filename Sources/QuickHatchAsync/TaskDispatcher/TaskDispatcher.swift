//
//  TaskDispatcher.swift
//  QuickHatchAsync
//
//  Created by Daniel Koster on 6/9/26.
//
import Foundation

public protocol TaskCoalescing: Sendable {
    func execute<Value: Sendable>(
        id: String,
        evictionTimeout: Duration,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value
}

public protocol TaskSerializing: Sendable {
    func execute<Value: Sendable>(
        id: String,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value
}

public enum TaskError: Error {
    case timeout
}
