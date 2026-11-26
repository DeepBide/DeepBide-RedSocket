"""Exercise the actual CET wrapper with a native/proxy stub, not the game."""
from pathlib import Path
import unittest

from lupa.luajit21 import LuaRuntime

CET = Path(__file__).resolve().parents[1] / "scripts/cet"

STUB = r'''
events={}; handles={}; callbacks=0; fail_destroy=false; fail_create=false; reject_listener=false
Socket={}
function Socket.Create()
  if fail_create then return nil end
  local h={destroys=0,disconnects=0,connects=0,sends=0,registrations=0}
  function h:IsRetired()return self.destroys>0 end
  function h:IsConnected()return self.destroys==0 end
  function h:Connect(...)self.connects=self.connects+1 end
  function h:Disconnect()self.disconnects=self.disconnects+1 end
  function h:SendCommand(...)self.sends=self.sends+1 end
  function h:TryRegisterListener(target,...)
    if reject_listener then return false end
    self.target=target;self.registrations=self.registrations+1;return true
  end
  handles[#handles+1]=h
  return h
end
function Socket.Destroy(h)
  h.destroys=h.destroys+1
  if h.target then h.target.OnDisconnection.callback();h.target.OnCommand.callback("late") end
  if fail_destroy and h==handles[1] then error("synthetic native failure")end
end
function NewProxy(def)
  return {Target=function()return def end,Function=function(_,name)return name end}
end
function registerForEvent(name,fn)events[name]=fn end
function log_callback(...)callbacks=callbacks+1 end
'''


def load():
    lua=LuaRuntime(unpack_returned_tuples=True)
    lua.execute(STUB)
    lua.globals().klass=lua.execute((CET/"RedSocket.lua").read_text(encoding="utf-8-sig"))
    lua.execute("function require(name)assert(name=='RedSocket');return klass end")
    lua.globals().api=lua.execute((CET/"init.lua").read_text(encoding="utf-8-sig"))
    return lua


class CetOwnershipTests(unittest.TestCase):
    def test_individual_destroy_and_shutdown_are_exactly_once(self):
        lua=load()
        self.assertEqual(lua.execute('''
          a=api.createSocket();b=api.createSocket()
          a:RegisterListener(log_callback,log_callback,log_callback,log_callback)
          a:Disconnect()
          api.destroySocket(a);api.destroySocket(a)
          local b_before=handles[2].destroys
          events.onShutdown();events.onShutdown()
          return handles[1].destroys,handles[2].destroys,b_before,callbacks
        '''),(1,1,0,0))

    def test_direct_destroy_retires_wrapper_and_callbacks(self):
        lua=load()
        self.assertEqual(lua.execute('''
          a=api.createSocket()
          a:RegisterListener(log_callback,log_callback,log_callback,log_callback)
          local native=a.socket
          a:Destroy();api.destroySocket(a);a:Destroy()
          a:Connect("127.0.0.1",20770);a:SendCommand("x");a:Disconnect()
          native.target.OnError.callback()
          events.onShutdown()
          return native.destroys,native.connects,native.sends,native.disconnects,callbacks,a:IsConnected()
        '''),(1,0,0,0,0,False))

    def test_shutdown_isolation_after_native_failure(self):
        lua=load()
        self.assertEqual(lua.execute('''
          a=api.createSocket();b=api.createSocket();fail_destroy=true
          local ok=pcall(events.onShutdown)
          events.onShutdown()
          local refused=api.createSocket()==nil
          return ok,handles[1].destroys,handles[2].destroys,refused
        '''),(True,1,1,True))

    def test_missing_native_and_proxy(self):
        lua=load()
        self.assertEqual(lua.execute('''
          fail_create=true;local absent=api.createSocket()==nil
          fail_create=false;a=api.createSocket()
          local ok=pcall(function()a:SendCommand("x",3)end)
          events.onShutdown()
          return absent,ok,handles[1].sends,handles[1].destroys
        '''),(True,True,0,1))

    def test_listener_rejection_preserves_existing_proxy(self):
        lua=load()
        self.assertEqual(lua.execute("""
          a=api.createSocket()
          local first=a:RegisterListener(log_callback,log_callback,nil,log_callback)
          local prior=a.proxy;reject_listener=true
          local second=a:RegisterListener(function()callbacks=callbacks+100 end,log_callback)
          handles[1].target.OnCommand.callback("line")
          return first,second,a.proxy==prior,callbacks,handles[1].registrations
        """),(True,False,True,1,1))

    def test_native_retirement_is_visible_and_pruned(self):
        lua=load()
        self.assertEqual(lua.execute("""
          a=api.createSocket();a:RegisterListener(log_callback,log_callback)
          local native=a.socket;native.destroys=1
          local retired=a:IsRetired();local refused=a:SendCommand("x")==false
          b=api.createSocket()
          return retired,refused,a.socket==nil,b~=nil
        """),(True,True,True,True))

    def test_foreign_release_cannot_destroy_unowned_wrapper(self):
        lua=load()
        self.assertEqual(lua.execute('''
          local foreign=klass:new()
          api.destroySocket(foreign)
          events.onShutdown()
          return handles[1].destroys
        '''),0)


if __name__=="__main__":
    unittest.main()
