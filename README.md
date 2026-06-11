# QuickHatchAsync

`QuickHatchAsync` is a lightweight, high-performance Swift Concurrency utility kit designed to solve advanced asynchronous execution patterns. It provides thread-safe primitives built on top of modern native tools like structured concurrency and `OSAllocatedUnfairLock` to coordinate identical or conflicting parallel operations seamlessly.

## 🚀 Features

* **TaskCoalescer**: Request deduplication and optimization. Coalesces simultaneous identical requests into a single flight, tracks caller references for cooperative cancellation, and auto-evicts stalled tasks via durations.
* **TaskSerializer**: Execution pacing and throttling. Aggressively enforces a "latest-win" policy by canceling active in-flight operations the moment a new one arrives with the same identifier.
* **100% Thread-Safe**: Avoids heavy actors or serial queues by using low-overhead synchronization wrappers (`OSAllocatedUnfairLock`).

---

## 🛠 Installation

Add the package dependency to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/dkoster95/QuickHatchAsync", from: "1.0.0")
]
```

---

## 📖 Component Guide & Code Samples

### 1. TaskCoalescer

Use `TaskCoalescer` when multiple distinct parts of your application independently ask for the exact same resource simultaneously (e.g., cell recycling in a list triggering duplicate network requests for the same image or user profile).

#### Key Behaviors:
* **Coalescing**: If 5 places request ID `"user_profile_42"` concurrently, only **one** async network operation is executed. All 5 callers await and receive the same identical result.
* **Reference-Counted Cancellation**: If Caller A cancels their view context, the background operation keeps running for Caller B. The underlying operation is forcefully aborted *only* when all callers cancel.
* **Timeout Eviction**: Tasks stuck in-flight longer than the specified threshold are evicted from the shared buffer to prevent caching corrupted or stalled connections.

#### Code Sample:
```swift
import Foundation
import QuickHatchAsync

struct UserService {
    private let coalescer = TaskCoalescer.shared
    
    func fetchUserProfile(id: String) async throws -> UserProfile {
        // Coalesces matching operations, with an optional 15-second expiration safety net
        try await coalescer.execute(id: "profile_\(id)", evictionTimeout: .seconds(15)) {
            print("🛫 Initialized network dispatch for user \(id) - only runs once!")
            
            let url = URL(string: "https://example.com\(id)")!
            let (data, _) = try await URLSession.shared.data(from: url)
            return try JSONDecoder().decode(UserProfile.self, from: data)
        }
    }
}

// Simulated Usage: Parallel calls do not duplicate workloads
Task {
    // These calls fire concurrently, but only one actual request goes out
    async let userA = UserService().fetchUserProfile(id: "42")
    async let userB = UserService().fetchUserProfile(id: "42")
    async let userC = UserService().fetchUserProfile(id: "42")
    
    let profiles = try await [userA, userB, userC]
    print("Received \(profiles.count) matching profiles from 1 network call.")
}
```

---

### 2. TaskSerializer

Use `TaskSerializer` when dealing with user-driven events where only the *most recent* action matters, and older pending operations are instantly outdated (e.g., search typeahead/autocomplete textbars, pagination scrolling, or pull-to-refresh hammers).

#### Key Behaviors:
* **Aggressive Cancellation**: When a new execution request arrives under an existing ID, the previous running task is aborted via Swift's cooperative `Task.cancel()`.
* **Zero Debounce Latency**: Instantly starts the new task without waiting for the canceled task to clean up or finish its teardown stack.
* **Insulated State**: Designed with testable initializers to clear state across unit test cycles effortlessly.

#### Code Sample:
```swift
import Foundation
import QuickHatchAsync

@MainActor
class SearchViewModel: ObservableObject {
    @Published var suggestions: [String] = []
    
    // Isolated instance prevents global state contamination 
    private let serializer = TaskSerializer() 
    
    func queryDidChange(to newText: String) {
        guard !newText.isEmpty else { return }
        
        Task {
            do {
                // If the user types "S", then "Sw", then "Swi", 
                // "S" and "Sw" are canceled instantly mid-flight.
                let results = try await serializer.execute(id: "search_query") {
                    // Simulating a dynamic network delay or search engine lookup
                    try await Task.sleep(for: .milliseconds(400)) 
                    return try await SearchEngine.fetch(query: newText)
                }
                
                self.suggestions = results
            } catch is CancellationError {
                // Gracefully swallowed: This query was superseded by a newer keypress
                print("⚠️ Search operation for '\(newText)' was overridden.")
            } catch {
                print("❌ Real error occurred: \(error)")
            }
        }
    }
}
```

---


## 🔒 Threading Model Details

Unlike architectures built using explicit `DispatchQueues` or global Swift Actors (which introduce unstructured context switching overhead), `QuickHatchAsync` leverages `OSAllocatedUnfairLock`. This grants atomic mutations over critical state dictionaries instantly without stepping out of the calling asynchronous runtime thread, maintaining consistent high throughput.

