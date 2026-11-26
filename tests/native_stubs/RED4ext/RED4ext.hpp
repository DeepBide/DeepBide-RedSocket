#pragma once
// Test-only API doubles, deliberately not a RED4ext/RedLib ABI implementation.
#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace Red {
struct CString {
    std::string text;
    CString()=default;
    CString(const char* p):text(p){}
    CString(const char* p,uint32_t n):text(p,n){}
    explicit CString(std::string v):text(std::move(v)){}
    const char* c_str() const noexcept {return text.c_str();}
    uint32_t Length() const noexcept {return static_cast<uint32_t>(text.size());}
};
struct CName {
    std::string text;
    CName()=default;
    CName(const char* p):text(p?p:""){}
    bool IsNone() const noexcept{return text.empty();}
};
using CallbackArgument=std::variant<std::monostate,int32_t,std::string>;
struct IScriptable {
    virtual ~IScriptable()=default;
    virtual void Invoke(const std::string&,const CallbackArgument&){}
};
template<class T> class Handle {
    template<class> friend class Handle;
    std::shared_ptr<T> ptr;
public:
    Handle()=default;
    Handle(std::nullptr_t){}
    explicit Handle(std::shared_ptr<T> p):ptr(std::move(p)){}
    template<class U> requires std::is_convertible_v<U*,T*>
    Handle(const Handle<U>& p):ptr(p.ptr){}
    T* GetPtr() const noexcept{return ptr.get();}
    T* operator->() const noexcept{return ptr.get();}
    explicit operator bool() const noexcept{return static_cast<bool>(ptr);}
    bool operator==(const Handle& p)const noexcept{return ptr==p.ptr;}
};
template<class T,class... A> Handle<T> MakeHandle(A&&... a) {
    return Handle<T>(std::make_shared<T>(std::forward<A>(a)...));
}
template<class T,int Default=0> struct Optional {
    T value{};
    Optional() {if constexpr(std::is_integral_v<T>) value=Default;}
    Optional(T v):value(std::move(v)){}
};
inline void CallVirtual(IScriptable* obj,const CName& name) {
    if(!obj) throw std::runtime_error("null test target");
    obj->Invoke(name.text,std::monostate{});
}
inline void CallVirtual(IScriptable* obj,const CName& name,int32_t v) {
    if(!obj) throw std::runtime_error("null test target");
    obj->Invoke(name.text,v);
}
inline void CallVirtual(IScriptable* obj,const CName& name,const CString& v) {
    if(!obj) throw std::runtime_error("null test target");
    obj->Invoke(name.text,v.text);
}
}
namespace RED4ext::v1 {
using PluginHandle=void*;
struct Logger {
    inline static std::mutex mutex;
    inline static std::vector<std::string> lines;
    void Add(const char* p) const {std::lock_guard lock(mutex);lines.emplace_back(p);}
    void Trace(PluginHandle,const char* p)const{Add(p);}
    void Debug(PluginHandle,const char* p)const{Add(p);}
    void Info(PluginHandle,const char* p)const{Add(p);}
    void Warn(PluginHandle,const char* p)const{Add(p);}
    void Error(PluginHandle,const char* p)const{Add(p);}
    void Critical(PluginHandle,const char* p)const{Add(p);}
};
struct Sdk {const Logger* logger;};
}
#define RTTI_IMPL_TYPEINFO(...)
#define RTTI_IMPL_ALLOCATOR(...)
#define RTTI_DEFINE_CLASS(...)
