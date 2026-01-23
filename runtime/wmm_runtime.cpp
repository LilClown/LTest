#include "include/wmm_runtime.h"

#include <atomic>
#include <type_traits>

#include "include/lib.h"
#include "include/wmm.h"

namespace {

template <typename T>
int GetLocationId(void* addr, T initial_value) {
  auto& graph = ExecutionGraph::getInstance();
  return graph.GetOrRegisterLocation<T>(addr, initial_value);
}

template <typename T>
T LoadImpl(void* addr, int order) {
  auto& graph = ExecutionGraph::getInstance();
  int location_id = GetLocationId<T>(addr, T{});
  if (this_coro) {
    return graph.Load<T>(
        location_id,
        this_thread_id,
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(order)));
  }
  auto* atom = reinterpret_cast<std::atomic<T>*>(addr);
  return atom->load(static_cast<std::memory_order>(order));
}

template <typename T>
void StoreImpl(void* addr, int order, T value) {
  auto& graph = ExecutionGraph::getInstance();
  int location_id = GetLocationId<T>(addr, value);
  if (this_coro) {
    graph.Store<T>(
        location_id,
        this_thread_id,
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(order)),
        value);
    return;
  }
  auto* atom = reinterpret_cast<std::atomic<T>*>(addr);
  atom->store(value, static_cast<std::memory_order>(order));
}

template <typename T>
struct CmpXchgResult {
  T old;
  bool success;
};

template <typename T>
CmpXchgResult<T> CmpXchgImpl(
    void* addr,
    T expected,
    T desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto& graph = ExecutionGraph::getInstance();
  int location_id = GetLocationId<T>(addr, expected);

  if (this_coro) {
    T expected_copy = expected;
    auto [rmw_success, read_value] = graph.ReadModifyWrite<T>(
        location_id,
        this_thread_id,
        &expected_copy,
        desired,
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(success_order)),
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(failure_order)));
    return CmpXchgResult<T>{read_value, rmw_success};
  }

  auto* atom = reinterpret_cast<std::atomic<T>*>(addr);
  T expected_copy = expected;
  bool success = is_weak
                     ? atom->compare_exchange_weak(
                           expected_copy, desired,
                           static_cast<std::memory_order>(success_order),
                           static_cast<std::memory_order>(failure_order))
                     : atom->compare_exchange_strong(
                           expected_copy, desired,
                           static_cast<std::memory_order>(success_order),
                           static_cast<std::memory_order>(failure_order));
  return CmpXchgResult<T>{expected_copy, success};
}

template <typename T>
T AtomicRmwImpl(void* addr, int order, int op, T operand) {
  auto& graph = ExecutionGraph::getInstance();
  int location_id = GetLocationId<T>(addr, T{});

  if (this_coro) {
    T expected = T{};
    T desired = expected;
    switch (op) {
      case LTEST_RMW_ADD: desired = expected + operand; break;
      case LTEST_RMW_SUB: desired = expected - operand; break;
      case LTEST_RMW_AND: desired = expected & operand; break;
      case LTEST_RMW_OR: desired = expected | operand; break;
      case LTEST_RMW_XOR: desired = expected ^ operand; break;
      case LTEST_RMW_XCHG: desired = operand; break;
      case LTEST_RMW_MAX: desired = (expected > operand) ? expected : operand; break;
      case LTEST_RMW_MIN: desired = (expected < operand) ? expected : operand; break;
      case LTEST_RMW_UMAX: {
        using U = std::make_unsigned_t<T>;
        desired = (static_cast<U>(expected) > static_cast<U>(operand)) ? expected : operand;
        break;
      }
      case LTEST_RMW_UMIN: {
        using U = std::make_unsigned_t<T>;
        desired = (static_cast<U>(expected) < static_cast<U>(operand)) ? expected : operand;
        break;
      }
      default: desired = operand; break;
    }

    auto rmw_result = graph.ReadModifyWrite<T>(
        location_id,
        this_thread_id,
        &expected,
        desired,
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(order)),
        WmmUtils::OrderFromStd(static_cast<std::memory_order>(order)));
    return rmw_result.second;
  }

  auto* atom = reinterpret_cast<std::atomic<T>*>(addr);
  auto order_mo = static_cast<std::memory_order>(order);
  switch (op) {
    case LTEST_RMW_ADD: return atom->fetch_add(operand, order_mo);
    case LTEST_RMW_SUB: return atom->fetch_sub(operand, order_mo);
    case LTEST_RMW_AND: return atom->fetch_and(operand, order_mo);
    case LTEST_RMW_OR: return atom->fetch_or(operand, order_mo);
    case LTEST_RMW_XOR: return atom->fetch_xor(operand, order_mo);
    case LTEST_RMW_XCHG: return atom->exchange(operand, order_mo);
    default: {
      T cur = atom->load(order_mo);
      while (true) {
        T next = cur;
        switch (op) {
          case LTEST_RMW_MAX: next = (cur > operand) ? cur : operand; break;
          case LTEST_RMW_MIN: next = (cur < operand) ? cur : operand; break;
          case LTEST_RMW_UMAX: {
            using U = std::make_unsigned_t<T>;
            next = (static_cast<U>(cur) > static_cast<U>(operand)) ? cur : operand;
            break;
          }
          case LTEST_RMW_UMIN: {
            using U = std::make_unsigned_t<T>;
            next = (static_cast<U>(cur) < static_cast<U>(operand)) ? cur : operand;
            break;
          }
          default: next = operand; break;
        }
        if (atom->compare_exchange_weak(cur, next, order_mo, order_mo)) {
          return cur;
        }
      }
    }
  }
}

}  // namespace

extern "C" bool __ltest_wmm_load_i1(void* addr, int order) {
  return LoadImpl<bool>(addr, order);
}

extern "C" int8_t __ltest_wmm_load_i8(void* addr, int order) {
  return LoadImpl<int8_t>(addr, order);
}

extern "C" int16_t __ltest_wmm_load_i16(void* addr, int order) {
  return LoadImpl<int16_t>(addr, order);
}

extern "C" int32_t __ltest_wmm_load_i32(void* addr, int order) {
  return LoadImpl<int32_t>(addr, order);
}

extern "C" int64_t __ltest_wmm_load_i64(void* addr, int order) {
  return LoadImpl<int64_t>(addr, order);
}


extern "C" void __ltest_wmm_store_i1(void* addr, int order, bool value) {
  StoreImpl<bool>(addr, order, value);
}

extern "C" void __ltest_wmm_store_i8(void* addr, int order, int8_t value) {
  StoreImpl<int8_t>(addr, order, value);
}

extern "C" void __ltest_wmm_store_i16(void* addr, int order, int16_t value) {
  StoreImpl<int16_t>(addr, order, value);
}

extern "C" void __ltest_wmm_store_i32(void* addr, int order, int32_t value) {
  StoreImpl<int32_t>(addr, order, value);
}

extern "C" void __ltest_wmm_store_i64(void* addr, int order, int64_t value) {
  StoreImpl<int64_t>(addr, order, value);
}


extern "C" LTestCmpXchgResult_i1 __ltest_wmm_cmpxchg_i1(
    void* addr,
    bool expected,
    bool desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto result = CmpXchgImpl<bool>(
      addr,
      expected,
      desired,
      success_order,
      failure_order,
      is_weak);
  return {result.old, result.success};
}

extern "C" LTestCmpXchgResult_i8 __ltest_wmm_cmpxchg_i8(
    void* addr,
    int8_t expected,
    int8_t desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto result = CmpXchgImpl<int8_t>(
      addr,
      expected,
      desired,
      success_order,
      failure_order,
      is_weak);
  return {result.old, result.success};
}

extern "C" LTestCmpXchgResult_i16 __ltest_wmm_cmpxchg_i16(
    void* addr,
    int16_t expected,
    int16_t desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto result = CmpXchgImpl<int16_t>(
      addr,
      expected,
      desired,
      success_order,
      failure_order,
      is_weak);
  return {result.old, result.success};
}

extern "C" LTestCmpXchgResult_i32 __ltest_wmm_cmpxchg_i32(
    void* addr,
    int32_t expected,
    int32_t desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto result = CmpXchgImpl<int32_t>(
      addr,
      expected,
      desired,
      success_order,
      failure_order,
      is_weak);
  return {result.old, result.success};
}

extern "C" LTestCmpXchgResult_i64 __ltest_wmm_cmpxchg_i64(
    void* addr,
    int64_t expected,
    int64_t desired,
    int success_order,
    int failure_order,
    bool is_weak) {
  auto result = CmpXchgImpl<int64_t>(
      addr,
      expected,
      desired,
      success_order,
      failure_order,
      is_weak);
  return {result.old, result.success};
}


extern "C" int8_t __ltest_wmm_rmw_i8(
    void* addr, int order, int op, int8_t operand) {
  return AtomicRmwImpl<int8_t>(addr, order, op, operand);
}

extern "C" int16_t __ltest_wmm_rmw_i16(
    void* addr, int order, int op, int16_t operand) {
  return AtomicRmwImpl<int16_t>(addr, order, op, operand);
}

extern "C" int32_t __ltest_wmm_rmw_i32(
    void* addr, int order, int op, int32_t operand) {
  return AtomicRmwImpl<int32_t>(addr, order, op, operand);
}

extern "C" int64_t __ltest_wmm_rmw_i64(
    void* addr, int order, int op, int64_t operand) {
  return AtomicRmwImpl<int64_t>(addr, order, op, operand);
}
