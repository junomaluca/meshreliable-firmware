#include "Lock.h"
#include "configuration.h"
#include <cassert>

namespace concurrency
{

#ifdef HAS_FREE_RTOS
Lock::Lock() : handle(xSemaphoreCreateRecursiveMutex())
{
    assert(handle);
}

Lock::~Lock()
{
    vSemaphoreDelete(handle);
}

void Lock::lock()
{
    if (xSemaphoreTakeRecursive(handle, portMAX_DELAY) == false) {
        abort();
    }
}

void Lock::unlock()
{
    if (xSemaphoreGiveRecursive(handle) == false) {
        abort();
    }
}
#else
Lock::Lock() {}

Lock::~Lock() {}

void Lock::lock() {}

void Lock::unlock() {}
#endif

} // namespace concurrency
