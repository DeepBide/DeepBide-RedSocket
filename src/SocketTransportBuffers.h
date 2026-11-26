#pragma once

#include <asio.hpp>
#include <array>
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include "SocketResourceBudget.h"

// Asio retains buffer descriptors, not the referenced storage.
template<class Buffer, class Completion>
auto RetainSocketBuffer(const std::shared_ptr<Buffer>& buffer, Completion completion) {
    return [buffer, completion = std::move(completion)](const asio::error_code& error, std::size_t length) mutable {
        completion(buffer, error, length);
    };
}

enum class SocketLineResult { NeedMore, Ready, TooLarge, Malformed };

// Strict UTF-8 (including empty line); no NUL or in-band CR/LF. Reject overlong
// encodings, surrogate code points and values outside the Unicode range.
inline bool IsSingleSocketLine(std::string_view command) noexcept {
    for (std::size_t i = 0; i < command.size();) {
        const auto c = static_cast<unsigned char>(command[i++]);
        if (c == 0 || c == '\r' || c == '\n') return false;
        if (c < 0x80) continue;
        unsigned count = 0;
        unsigned code = 0, min = 0;
        if (c >= 0xc2 && c <= 0xdf) { count=1; code=c&0x1f; min=0x80; }
        else if (c >= 0xe0 && c <= 0xef) { count=2; code=c&0x0f; min=0x800; }
        else if (c >= 0xf0 && c <= 0xf4) { count=3; code=c&0x07; min=0x10000; }
        else return false;
        if (count > command.size()-i) return false;
        while (count--) {
            const auto tail = static_cast<unsigned char>(command[i++]);
            if ((tail & 0xc0) != 0x80) return false;
            code = (code << 6) | (tail & 0x3f);
        }
        if (code < min || code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) return false;
    }
    return true;
}

// The scan cursor advances monotonically, including fragmented max-size lines.
// Consumed coalesced lines are compacted once before the next receive.
class SocketLineReader {
public:
    void Append(const char* bytes, std::size_t size) { m_buffer.append(bytes, size); }
    SocketLineResult Take(std::string& line, std::size_t limit) {
        const auto end = m_buffer.find('\n', m_scan);
        const auto scannedEnd = end == std::string::npos ? m_buffer.size() : end+1;
        m_scanned += scannedEnd - m_scan;
        m_scan = scannedEnd;
        if (end == std::string::npos) {
            const auto size = m_buffer.size() - m_begin;
            if (size > limit && (size-limit != 1 || m_buffer.back() != '\r'))
                return SocketLineResult::TooLarge;
            if (m_begin != 0) {
                m_buffer.erase(0, m_begin);
                m_scan -= m_begin;
                m_begin = 0;
            }
            return SocketLineResult::NeedMore;
        }
        const auto length = end - m_begin - (end != m_begin && m_buffer[end-1] == '\r' ? 1 : 0);
        if (length > limit) return SocketLineResult::TooLarge;
        const std::string_view value(m_buffer.data()+m_begin,length);
        if (!IsSingleSocketLine(value)) return SocketLineResult::Malformed;
        line.assign(value);
        m_begin = end+1;
        return SocketLineResult::Ready;
    }
    std::size_t ScannedBytes() const noexcept { return m_scanned; }
private:
    std::string m_buffer;
    std::size_t m_begin{0}, m_scan{0}, m_scanned{0};
};

struct SocketWritePayload {
    static constexpr std::size_t kMaxEntries = 512;
    std::string Text;
    std::shared_ptr<const SocketResourceBudget::Reservation> Charge;
    static std::shared_ptr<const SocketWritePayload> Create(std::string message) {
        auto charge = SocketResourceBudget::Reserve(message.size());
        if (!charge) return {};
        return std::make_shared<const SocketWritePayload>(SocketWritePayload{std::move(message),std::move(charge)});
    }
};

// Every composed operation owns its own transport epoch, not merely its
// buffer. A stale Asio continuation can never touch a replacement socket.
struct SocketTransportEpoch {
    static constexpr std::size_t kReadChunk = 4096;
    template<class Executor>
    explicit SocketTransportEpoch(const Executor& executor) : Resolver(executor), Socket(executor) {}
    asio::ip::tcp::resolver Resolver;
    asio::ip::tcp::socket Socket;
    asio::cancellation_signal ConnectCancel, WriteCancel;
    std::array<char, kReadChunk> ReadBuffer{};
    SocketLineReader Lines;
    void Close() noexcept {
        try { ConnectCancel.emit(asio::cancellation_type::all); } catch (...) {}
        try { WriteCancel.emit(asio::cancellation_type::all); } catch (...) {}
        try { Resolver.cancel(); } catch (...) {}
        asio::error_code ignored;
        Socket.cancel(ignored);
        Socket.close(ignored);
    }
};
