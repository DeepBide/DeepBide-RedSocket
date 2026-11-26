#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

// Owns references that may be invalidated by arbitrary producer threads.
// RequestClear never destroys a value; Drain* transfers ownership to the
// caller, which must run on the designated release context.
template<class THandle, class TMetadata>
class DeferredHandleRegistry {
public:
    struct Entry {
        THandle Handle;
        TMetadata Metadata;
        bool ClearRequested{false};
    };

    bool Insert(uint64_t p_id, THandle&& p_handle, TMetadata&& p_metadata, std::size_t p_limit) {
        std::lock_guard lock(m_mutex);
        if (m_entries.size() >= p_limit || m_entries.contains(p_id)) return false;
        m_entries.emplace(p_id, Entry{std::move(p_handle), std::move(p_metadata), false});
        return true;
    }

    // Safe from worker threads: this only changes a flag under the registry lock.
    bool RequestClear(uint64_t p_id) {
        if (p_id == 0) return false;
        std::lock_guard lock(m_mutex);
        const auto it = m_entries.find(p_id);
        if (it == m_entries.end()) return false;
        it->second.ClearRequested = true;
        return true;
    }

    template<class Predicate>
    void RequestClearIf(Predicate&& predicate) {
        std::lock_guard lock(m_mutex);
        for (auto& [id, entry] : m_entries) {
            (void)id;
            if (predicate(entry.Metadata)) entry.ClearRequested = true;
        }
    }

    bool Take(uint64_t p_id, Entry& p_entry) {
        std::lock_guard lock(m_mutex);
        const auto it = m_entries.find(p_id);
        if (it == m_entries.end() || it->second.ClearRequested) return false;
        p_entry = std::move(it->second);
        m_entries.erase(it);
        return true;
    }

    std::vector<Entry> DrainCleared() {
        std::vector<Entry> drained;
        std::lock_guard lock(m_mutex);
        for (auto it = m_entries.begin(); it != m_entries.end();) {
            if (!it->second.ClearRequested) { ++it; continue; }
            drained.push_back(std::move(it->second));
            it = m_entries.erase(it);
        }
        return drained;
    }

    // Terminal bulk cleanup transfers the map itself, with no per-entry
    // allocation. Destruction happens after the registry lock is released.
    void ReleaseAll() {
        decltype(m_entries) released;
        { std::lock_guard lock(m_mutex); released.swap(m_entries); }
    }

    std::vector<Entry> DrainAll() {
        std::vector<Entry> drained;
        std::lock_guard lock(m_mutex);
        drained.reserve(m_entries.size());
        for (auto& [id, entry] : m_entries) {
            (void)id;
            drained.push_back(std::move(entry));
        }
        m_entries.clear();
        return drained;
    }

    std::size_t Size() const {
        std::lock_guard lock(m_mutex);
        return m_entries.size();
    }

private:
    mutable std::mutex m_mutex;
    std::unordered_map<uint64_t, Entry> m_entries;
};
