#pragma once

#include <algorithm>
#include <atomic>
#include <iostream>
#include <ostream>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>
#include <cassert>
#include <map>
#include <ranges>


enum class ReadStrategy {
    Random,
    PCT,
};

template<class T>
struct ReadCandidate {
    EventId writeId;
    T value;
    Event* writeEvent;
    
    ReadCandidate(EventId id, T val, Event* evt) 
        : writeId(id), value(val), writeEvent(evt) {}
};

enum class MemoryOrder {
  Relaxed,
  Acquire,
  Release,
  AcqRel,
  SeqCst
};

namespace { // translation-unit-local details
struct HBClock {
  HBClock() = default;
  HBClock(int nThreads): times(nThreads, 0) {}

  std::string AsString() const {
    std::stringstream ss;

    ss << "[";
    for (size_t i = 0; i < times.size(); ++i) {
      ss << times[i];
      if (i < times.size() - 1) {
        ss << ",";
      }
    }
    ss << "]";

    return ss.str();
  }

  bool IsSubsetOf(const HBClock& other) const {
    assert(IsSameLength(other));
    
    for (int i = 0; i < times.size(); ++i) {
      if (times[i] > other.times[i]) {
        return false;
      }
    }

    return true;
  }

  void UniteWith(const HBClock& other) {
    assert(IsSameLength(other));

    for (int i = 0; i < times.size(); ++i) {
      times[i] = std::max(times[i], other.times[i]);
    }
  }

  void Increment(int threadId) {
    assert(threadId >= 0 && threadId < times.size());
    times[threadId]++;
  }

private:
  bool IsSameLength(const HBClock& other) const {
    return times.size() == other.times.size();
  }

  std::vector<int> times;
};
  

using EdgeId = int;
using EventId = int;

enum class EdgeType {
  PO, // program order / sequenced before
  SC, // seq-cst edge
  RF, // reads-from
  // TODO: do we need it? since we have hb-clocks already
  // HB, // happens-before
  MO, // modification order
  // TODO: do we need it explicitly? hb-clocks can give the same basically
  // SW, // synchronized-with 
};



struct Edge {
  EdgeId id;
  EdgeType type;
  EventId from;
  EventId to;
};

enum class EventType {
  DUMMY,
  READ,
  WRITE,
  RMW
};
}

struct WmmUtils {
  inline static MemoryOrder OrderFromStd(std::memory_order order) {
    switch (order) {
      case std::memory_order_relaxed:
        return MemoryOrder::Relaxed;
      case std::memory_order_acquire:
        return MemoryOrder::Acquire;
      case std::memory_order_release:
        return MemoryOrder::Release;
      case std::memory_order_acq_rel:
        return MemoryOrder::AcqRel;
      case std::memory_order_seq_cst:
        return MemoryOrder::SeqCst;
      default:
        throw std::invalid_argument("Unsupported memory order");
    }
  }

  inline static std::string OrderToString(MemoryOrder order) {
    switch (order) {
      case MemoryOrder::Relaxed:
        return "Relaxed";
      case MemoryOrder::Acquire:
        return "Acquire";
      case MemoryOrder::Release:
        return "Release";
      case MemoryOrder::AcqRel:
        return "AcqRel";
      case MemoryOrder::SeqCst:
        return "SeqCst";
    }
  }

  inline static std::string EventTypeToString(EventType type) {
    switch (type) {
      case EventType::DUMMY: return "D";
      case EventType::READ: return "R"; 
      case EventType::WRITE: return "W";
      case EventType::RMW: return "RMW";
    }
  }

  inline static std::string EdgeTypeToString(EdgeType type) {
    switch (type) {
      case EdgeType::PO: return "po";
      case EdgeType::SC: return "sc";
      case EdgeType::RF: return "rf";
      // case EdgeType::HB: return "hb";
      case EdgeType::MO: return "mo";
      //case EdgeType::SW: return "sw";
    }
  }

  // thread id to which all initalization events (constructors of atomics) will belong to
  inline static int INIT_THREAD_ID = 0;
};

namespace { // translation-unit-local details

struct Event {
protected:
  Event(EventId id, EventType type, int nThreads, int location, int threadId, MemoryOrder order):
    id(id), type(type), location(location), threadId(threadId), order(order), clock(nThreads) {}

public:
  EventId id;
  EventType type;
  int location;
  int threadId;
  MemoryOrder order;
  HBClock clock;
  std::vector<EdgeId> edges; // outgoing edges (e.g. `edge.to == this`)
  
  virtual ~Event() = default;

  virtual std::string AsString() const {
    std::stringstream ss;

    ss << id << ":" << WmmUtils::EventTypeToString(type)
       << ":T" << threadId << ":L" << location << ":"
       << WmmUtils::OrderToString(order) << ":" << clock.AsString();

    return ss.str();
  }

  virtual void SetReadFromEvent(Event* event) {
    assert(false && "'SetReadFromEvent' can only be called on read/rmw events");
  }

  virtual Event* GetReadFromEvent() const {
    assert(false && "'GetReadFromEvent' can only be called on read/rmw events");
    return nullptr;
  }

  bool HappensBefore(Event* other) const {
    return clock.IsSubsetOf(other->clock);
  }

  bool IsDummy() const {
    return type == EventType::DUMMY;
  }

  bool IsWrite() const {
    return type == EventType::WRITE;
  }

  bool IsRead() const {
    return type == EventType::READ;
  }

  bool IsRMW() const {
    return type == EventType::RMW;
  }

  bool IsWriteOrRMW() const {
    return IsWrite() || IsRMW();
  }

  bool IsReadOrRMW() const {
    return IsRead() || IsRMW();
  }

  virtual bool IsSeqCst() const {
    return order == MemoryOrder::SeqCst;
  }

  virtual bool IsAcqRel() const {
    return order == MemoryOrder::AcqRel;
  }

  virtual bool IsAcquire() const {
    return order == MemoryOrder::Acquire;
  }

  virtual bool IsRelease() const {
    return order == MemoryOrder::Release;
  }

  virtual bool IsRelaxed() const {
    return order == MemoryOrder::Relaxed;
  }

  virtual bool IsAtLeastAcquire() const {
    return order >= MemoryOrder::Acquire && order != MemoryOrder::Release;
  }

  virtual bool IsAtLeastRelease() const {
    return order >= MemoryOrder::Release;
  }

  // Returns true if this event is a RMW and it is resolved to a READ state
  virtual bool IsReadRWM() const {
    return false;
  }

  // Returns true if this event is a RMW and it is resolved to a MODIFY state
  virtual bool IsModifyRMW() const {
    return false;
  }

  template<class T>
  static T GetWrittenValue(Event* event);

  template<class T>
  static T GetReadValue(Event* event);
};

struct DummyEvent : Event {
  DummyEvent(EventId id, int nThreads, int threadId):
    Event(id, EventType::DUMMY, nThreads, -1 /* non-existing location */, threadId, MemoryOrder::Relaxed) {}
};

template<class T>
struct WriteEvent : Event {
  WriteEvent(EventId id, int nThreads, int location, int threadId, MemoryOrder order, T value):
    Event(id, EventType::WRITE, nThreads, location, threadId, order), value(std::move(value)) {} // , moBefore(-1)

  virtual std::string AsString() const {
    std::stringstream ss;

    ss << id << ":" << WmmUtils::EventTypeToString(type) << "(" << value << ")"
       << ":T" << threadId << ":L" << location << ":"
       << WmmUtils::OrderToString(order) << ":" << clock.AsString();

    return ss.str();
  }

  T GetWrittenValue() const {
    return value;
  }
  
  T value;
};

template<class T>
struct ReadEvent : Event {
  ReadEvent(EventId id, int nThreads, int location, int threadId, MemoryOrder order):
    Event(id, EventType::READ, nThreads, location, threadId, order), readFrom(nullptr) {}
  
  virtual void SetReadFromEvent(Event* event) override {
    assert((event == nullptr || event->IsWriteOrRMW()) && "Read event must read from write/rmw event");
    readFrom = event;
  }

  virtual Event* GetReadFromEvent() const override {
    return readFrom;
  }
  
  T GetReadValue() const {
    assert(readFrom != nullptr && "Read event hasn't set its 'readFrom' write-event");
    return Event::GetWrittenValue<T>(readFrom);
  }

  // points to write-event which we read from
  Event* readFrom;
};

enum RMWState {
  READ,
  MODIFY,
  UNSET
};

template<class T>
struct RMWEvent : Event {
  RMWEvent(EventId id, int nThreads, int location, int threadId, T* expected, T desired,
           MemoryOrder successOrder, MemoryOrder failureOrder):
    Event(id, EventType::RMW, nThreads, location, threadId, successOrder), 
    // TODO: expcted might be uninitialized, so we should not dereference it blindly, fix it
    initialExpectedValue(*expected), cachedExpectedValue(*expected),
    expected(expected), desired(desired), failureOrder(failureOrder), readFrom(nullptr) {}

  virtual std::string AsString() const override {
    std::stringstream ss;

    ss << id << ":" << WmmUtils::EventTypeToString(type)
       << "(written_expected=" << cachedExpectedValue << ", init_expected=" << initialExpectedValue << ", desired=" << desired << ", " << StateAsString(state) << ")"
       << ":T" << threadId << ":L" << location << ":"
       << "succ=" << WmmUtils::OrderToString(order) << ", fail=" << WmmUtils::OrderToString(failureOrder)
       << ":" << clock.AsString();

    return ss.str();
  }

  /*
    All methods below will use a correct memory order of this RMW event,
    meaning that for 'state == READ' it will use 'failureOrder', and for 
    'state == MODIFY' it will use 'order' (successOrder). 
  */
  virtual bool IsSeqCst() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order == MemoryOrder::SeqCst;
  }

  virtual bool IsAcqRel() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order == MemoryOrder::AcqRel;
  }

  virtual bool IsAcquire() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order == MemoryOrder::Acquire;
  }

  virtual bool IsRelease() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order == MemoryOrder::Release;
  }

  virtual bool IsRelaxed() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order == MemoryOrder::Relaxed;
  }

  virtual bool IsAtLeastAcquire() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order >= MemoryOrder::Acquire && order != MemoryOrder::Release;
  }

  virtual bool IsAtLeastRelease() const override {
    assert(state != RMWState::UNSET && "RMW event's memory order is queried for its resolving");
    auto order = (state == RMWState::MODIFY) ? this->order : failureOrder;
    return order >= MemoryOrder::Release;
  }

  virtual bool IsReadRWM() const override {
    return state == RMWState::READ;
  }

  virtual bool IsModifyRMW() const override {
    return state == RMWState::MODIFY;
  }

  virtual void SetReadFromEvent(Event* event) override {
    assert((event == nullptr || event->IsWriteOrRMW()) && "Read event must read from write/rmw event");
    readFrom = event;

    if (readFrom == nullptr) {
      state = RMWState::UNSET;
      *expected = initialExpectedValue; // reset expected value
      cachedExpectedValue = initialExpectedValue;
    }
    else {
      T readValue = Event::GetWrittenValue<T>(readFrom);
      assert(*expected == initialExpectedValue && "Expected value must be equal to initial expected value on RMW resolving");
      if (readValue == initialExpectedValue) {
        // in case of MODIFY we do not change expected value
        state = RMWState::MODIFY;
      }
      else {
        // in case of READ we set expected to the actually read value
        state = RMWState::READ;
        *expected = readValue;
        cachedExpectedValue = readValue;
      }
    }
  }

  virtual Event* GetReadFromEvent() const override {
    return readFrom;
  }

  T GetReadValue() const {
    assert(readFrom != nullptr && "RMW event hasn't set its 'readFrom' write-event");
    return Event::GetWrittenValue<T>(readFrom);
  }

  T GetWrittenValue() const {
    assert(state != RMWState::UNSET && "RMW event must have a resolved state (not UNSET)");
    if (state == RMWState::MODIFY) {
      // in case of MODIFY we return desired value
      return desired;
    }
    else {
      // in case of READ we assume that RMW writes the value that it reads from its own 'readFrom' event
      // (which is saved to the *expected pointer)
      return *expected;
    }
  }

  RMWState state = UNSET; // current state of the RMW operation
  T initialExpectedValue;
  // TODO: I assume, that boost stack manipulation causes a failure when dereferencing of '*expected'
  // from different thread, and it causes sanitizer to fails. Requires inverstigation to debug it.
  T cachedExpectedValue;
  T* expected;
  T desired;
  MemoryOrder failureOrder;
  // points to write/rmw-event which we read from
  Event* readFrom;

private:
  inline static std::string StateAsString(RMWState state) {
    switch (state) {
      case RMWState::READ: return "READ";
      case RMWState::MODIFY: return "MODIFY";
      case RMWState::UNSET: return "UNSET";
    }
  }
};


template<class T>
T Event::GetWrittenValue(Event* event) {
  assert(event != nullptr && "Event must not be null");
  assert(event->IsWriteOrRMW() && "Only write/rmw events can write values");
  
  if (event->IsWrite()) {
    auto writeEvent = static_cast<WriteEvent<T>*>(event);
    return writeEvent->GetWrittenValue();
  }
  else {
    auto rmwEvent = static_cast<RMWEvent<T>*>(event);
    return rmwEvent->GetWrittenValue();
  }
}

template<class T>
T Event::GetReadValue(Event* event) {
  assert(event->IsReadOrRMW() && "Event must be a read/rmw event");
  if (event->IsRead()) {
    ReadEvent<T>* readEvent = static_cast<ReadEvent<T>*>(event);
    return readEvent->GetReadValue();
  }
  else {
    RMWEvent<T>* rmwEvent = static_cast<RMWEvent<T>*>(event);
    return rmwEvent->GetReadValue();
  }
}

}

class Graph {
public:

     ReadStrategy readStrategy = ReadStrategy::Random;

    template<class T>
    std::vector<ReadCandidate<T>> getReadCandidates(Event* readEvent) {
        std::vector<ReadCandidate<T>> candidates;
        
        for (Event* writeEvent : events) {
            if (!writeEvent->IsWriteOrRMW()) continue;
            if (writeEvent->location != readEvent->location) continue;
            if (writeEvent->id == readEvent->id) continue; 
            
            if (IsWriteVisibleToRead(writeEvent, readEvent)) {
                T value = Event::GetWrittenValue<T>(writeEvent);
                candidates.emplace_back(writeEvent->id, value, writeEvent);
            }
        }
        
        return candidates;
    }

    bool IsWriteVisibleToRead(Event* write, Event* read) {
        if (write->location != read->location) return false;
        
        if (write->IsRelaxed() && read->IsRelaxed()) return true;
        
        if (write->IsAtLeastRelease() && read->IsAtLeastAcquire()) {
            return true;         }
        
        if (write->IsSeqCst() && read->IsSeqCst()) {
            return true; 
        }
        
        return write->HappensBefore(read);
    }   


    template<class T, ReadStrategy S = ReadStrategy::Random>
    typename std::enable_if<S == ReadStrategy::Random, ReadCandidate<T>>::type
    makeChoice(std::vector<ReadCandidate<T>>& candidates) {
        if (candidates.empty()) {
            throw std::runtime_error("No read candidates available");
        }
        
        std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
        size_t idx = dist(gen);
        return candidates[idx];
    }
    
    template<class T, ReadStrategy S = ReadStrategy::Random>
    typename std::enable_if<S == ReadStrategy::PCT, ReadCandidate<T>>::type
    makeChoice(std::vector<ReadCandidate<T>>& candidates) {
        if (candidates.empty()) {
            throw std::runtime_error("No read candidates available");
        }
        
        std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1);
        size_t idx = dist(gen);
        return candidates[idx];
    }


  Graph() {}
  ~Graph() { Clean(); }  

  void Reset(int nThreads) {
    Clean();
    InitThreads(nThreads);
  }

  // TODO: add `ExecutionPolicy` or other way of specifying how to create edges (Random, BoundedModelChecker, etc.)
    template<class T>
    T AddReadEvent(int location, int threadId, MemoryOrder order) {
        EventId eventId = events.size();
        auto event = new ReadEvent<T>(eventId, nThreads, location, threadId, order);
        
        // Устанавливаем po-edge
        CreatePoEdgeToEvent(event);
        
        // Получаем кандидатов на чтение
        auto candidates = getReadCandidates<T>(event);
        
        if (candidates.empty()) {
            delete event;
            throw std::runtime_error("No available write events to read from");
        }
        
        // Выбираем кандидата в соответствии со стратегией
        ReadCandidate<T> chosenCandidate;
        if (readStrategy == ReadStrategy::Random) {
            chosenCandidate = makeChoice<T, ReadStrategy::Random>(candidates);
        } else if (readStrategy == ReadStrategy::PCT) {
            chosenCandidate = makeChoice<T, ReadStrategy::PCT>(candidates);
        }
        
        // Пытаемся создать rf-edge
        if (!TryCreateRfEdge(chosenCandidate.writeEvent, event)) {
            // Если не удалось, пробуем других кандидатов
            for (const auto& candidate : candidates) {
                if (candidate.writeId == chosenCandidate.writeId) continue;
                
                if (TryCreateRfEdge(candidate.writeEvent, event)) {
                    std::cout << "Read event " << event->AsString() 
                              << " now reads from " << candidate.writeEvent->AsString() << std::endl;
                    break;
                }
            }
        } else {
            std::cout << "Read event " << event->AsString() 
                      << " now reads from " << chosenCandidate.writeEvent->AsString() << std::endl;
        }
        
        assert(event->readFrom != nullptr && "Read event must have appropriate write event to read from");
        assert((event->readFrom->IsWrite() || event->readFrom->IsModifyRMW()) && 
               "Read event must read from write or modifying rmw event");
        
        return Event::GetReadValue<T>(event);
    }

  template<class T>
  void AddWriteEvent(int location, int threadId, MemoryOrder order, T value) {
    EventId eventId = events.size();
    auto event = new WriteEvent<T>(eventId, nThreads, location, threadId, order, value);

    // establish po-edge
    CreatePoEdgeToEvent(event); // prevInThread --po--> event
    
    if (order == MemoryOrder::SeqCst) {      
      // establish sc-edge between last sc-write and event
      CreateScEdgeToEvent(event); // prevScCstWrite --sc--> event

      // Seq-Cst / MO Consistency (if locations match)
      CreateSeqCstConsistencyEdges(event); // prevScCstWrite --mo--> event
      
      // update last seq_cst write
      lastSeqCstWriteEvents[event->location] = event->id;
    }

    // Read-Write Coherence
    CreateReadWriteCoherenceEdges(event);

    // Write-Write Coherence
    CreateWriteWriteCoherenceEdges(event);
  }

    template<class T>
    std::pair<bool, T> AddRMWEvent(int location, int threadId, T* expected, T desired,
                                   MemoryOrder successOrder, MemoryOrder failureOrder) {
        EventId eventId = events.size();
        auto event = new RMWEvent<T>(
            eventId, nThreads, location, threadId, expected, desired, successOrder, failureOrder
        );
        
        // Устанавливаем po-edge
        CreatePoEdgeToEvent(event);
        
        // Получаем кандидатов на чтение
        auto candidates = getReadCandidates<T>(event);
        
        if (candidates.empty()) {
            delete event;
            throw std::runtime_error("No available write events to read from for RMW");
        }
        
        // Выбираем кандидата в соответствии со стратегией
        ReadCandidate<T> chosenCandidate;
        if (readStrategy == ReadStrategy::Random) {
            chosenCandidate = makeChoice<T, ReadStrategy::Random>(candidates);
        } else if (readStrategy == ReadStrategy::PCT) {
            chosenCandidate = makeChoice<T, ReadStrategy::PCT>(candidates);
        }
        
        // Пытаемся создать rf-edge
        if (!TryCreateRfEdge(chosenCandidate.writeEvent, event)) {
            // Если не удалось, пробуем других кандидатов
            for (const auto& candidate : candidates) {
                if (candidate.writeId == chosenCandidate.writeId) continue;
                
                if (TryCreateRfEdge(candidate.writeEvent, event)) {
                    std::cout << "RMW event " << event->AsString() 
                              << " now reads from " << candidate.writeEvent->AsString() << std::endl;
                    break;
                }
            }
        } else {
            std::cout << "RMW event " << event->AsString() 
                      << " now reads from " << chosenCandidate.writeEvent->AsString() << std::endl;
        }
        
        assert(event->readFrom != nullptr && "RMW event must have appropriate write event to read from");
        assert((event->readFrom->IsWrite() || event->readFrom->IsModifyRMW()) && 
               "RMW event must read from write or modifying rmw event");
        
        return {
            event->IsModifyRMW(),
            Event::GetReadValue<T>(event)
        };
    }

  void Print(std::ostream& os) const {
    os << "Graph edges:" << std::endl;
    if (edges.empty()) os << "<empty>";
    else {
      for (const auto& edge : edges) {
        os << events[edge.from]->AsString() << " ->" << WmmUtils::EdgeTypeToString(edge.type) << " "
          << events[edge.to]->AsString() << std::endl;
      }
    }
    os << std::endl;
  }

private:

  void setReadStrategy(ReadStrategy strategy) {
        readStrategy = strategy;
    }

  std::vector<Event*> GetShuffledReadFromCandidates(Event* event) {
    // Shuffle events to randomize the order of read-from edges
    // and allow for more non-sc behaviours
    auto filteredEventsView = events | std::views::filter(
      [event](Event* e) {
        return (
          (e->IsWrite() || e->IsModifyRMW()) &&
          e->location == event->location &&
          e != event
        );
      }
    );
    std::vector<Event*> shuffledEvents(filteredEventsView.begin(), filteredEventsView.end());
    std::ranges::shuffle(shuffledEvents, gen);
    
    return shuffledEvents;
  }

  // Tries to create a read-from edge between `write` and `read` events (write --rf--> read).
  // Returns `true` if edge was created, `false` otherwise.
  bool TryCreateRfEdge(Event* write, Event* read) {
    assert(write->IsWriteOrRMW() && read->IsReadOrRMW() && "Write and Read events must be of correct type");
    assert(write->location == read->location && "Write and Read events must be of the same location");

    StartSnapshot();

    // establish rf-edge
    AddEdge(EdgeType::RF, write->id, read->id);
    // remember the event we read from
    read->SetReadFromEvent(write);

    // update the clock if synchronized-with relation has appeared
    // and save the old clock in case snapshot is discarded
    HBClock oldClock;
    bool isClockUpdated = false;

    // update the last seq-cst write event in case of successful RMW event
    // and save the old value in case snapshot is discarded
    EventId oldLastSeqCstWriteEvent = lastSeqCstWriteEvents[read->location];

    // Applying rel-acq semantics via establishing synchronized-with (SW) relation
    if (
      read->IsAtLeastAcquire() &&
      write->IsAtLeastRelease() &&
      // TODO: maybe somehow get rid of the sc-edges instead?
      !(read->IsSeqCst() && write->IsSeqCst()) // this case no need to consider, because we have sc edges instead
    ) {
      isClockUpdated = true;
      // save the old clock
      oldClock = read->clock;
      // instantitate a SW (synchronized-with) relation
      read->clock.UniteWith(write->clock);
    }

    if (read->IsRead() || read->IsReadRWM()) {
      // in case of 'read' event has actually type RMW but is resolved to READ state
      // (basically the comparison of read value with `expected` one failed), then we treat it as a regular READ event
      if (read->IsSeqCst()) {
        // establish sc-edge
        CreateScEdgeToEvent(read); // prevScCstWrite --sc--> event
        // Seq-Cst Write-Read Coherence
        CreateSeqCstReadWriteCoherenceEdges(write, read);
      }

      // Write-Read Coherence
      CreateWriteReadCoherenceEdges(write, read);

      // Read-Read Coherence
      CreateReadReadCoherenceEdges(write, read);
    }
    else {
      // otherwise, if the RWM event is resolved to MODIFY state
      // then we treat that as RMW event and apply rules from reads and writes as well
      assert(read->IsModifyRMW() && "Read event must be a successful RMW event in this code branch");
      
      // Apply RMW-specific rules
      // RMW / MO Consistency
      CreateRmwConsistencyEdges(write, read);

      // RMW Atomicity
      CreateRmwAtomicityEdges(write, read);

      // Applying rules both from READs and WRITEs
      if (read->IsSeqCst()) {
        // establish sc-edge
        CreateScEdgeToEvent(read); // prevScCstWrite --sc--> event
        // Seq-Cst Write-Read Coherence
        CreateSeqCstReadWriteCoherenceEdges(write, read);
        // Seq-Cst / MO Consistency (if locations match)
        CreateSeqCstConsistencyEdges(read); // prevScCstWrite --mo--> event
        
        // update last seq_cst write (will be restored in case of snapshot discard)
        lastSeqCstWriteEvents[read->location] = read->id;
      }

      // Write-Read Coherence
      CreateWriteReadCoherenceEdges(write, read);

      // Read-Read Coherence
      CreateReadReadCoherenceEdges(write, read);

      // Read-Write Coherence
      CreateReadWriteCoherenceEdges(read);

      // Write-Write Coherence
      CreateWriteWriteCoherenceEdges(read);
    }

    bool isConsistent = IsConsistent();   
    if (isConsistent) {
      std::cout << "Consistent graph:" << std::endl;
      Print(std::cout);
      // preserve added edges and other modifications
      ApplySnapshot();
    }
    else {
      std::cout << "Not consistent graph:" << std::endl;
      Print(std::cout);
      // removes all added edges
      std::cout << "Discarding snapshot" << std::endl;
      DiscardSnapshot();
      // remove rf-edge
      read->SetReadFromEvent(nullptr);
      // restore old clock if necessary
      if (isClockUpdated) {
        read->clock = oldClock;
      }
      // restore last seq-cst write event
      lastSeqCstWriteEvents[read->location] = oldLastSeqCstWriteEvent;
      Print(std::cout);
    }

    return isConsistent;
  }

  // ===== Methods to create general graph edges =====
  
  // Creates a po-edge between last event in the same thread as `event`
  void CreatePoEdgeToEvent(Event* event) {
    int threadId = event->threadId;
    EventId eventId = event->id;

    // connect prev event in the same thread with new event via PO edge
    auto lastEventInSameThread = events[eventsPerThread[threadId].back()];
    AddEdge(EdgeType::PO, lastEventInSameThread->id, eventId);
    
    // update last event in thread
    eventsPerThread[threadId].push_back(eventId);
    
    // insert in all-events vector
    events.push_back(event);

    // set correct hb-clocks for new event
    event->clock = lastEventInSameThread->clock;
    event->clock.Increment(threadId);
  }

  // Adds an sc-edge between prev sc-write (to the same location as `event`) and `event`
  void CreateScEdgeToEvent(Event* event) {
    assert(event->IsSeqCst() && "Event must be an SC access");

    // last seq_cst write should appear before us
    EventId lastSeqCstWriteEvent = GetLastSeqCstWriteEventId(event->location);
    if (lastSeqCstWriteEvent != -1) {
      auto lastSeqCstWrite = events[lastSeqCstWriteEvent];
      assert((lastSeqCstWrite->IsWrite() || lastSeqCstWrite->IsModifyRMW()) && "Prev scq-cst event must be a write or modifying rmw");
      AddEdge(EdgeType::SC, lastSeqCstWrite->id, event->id);
      
      // unite current hb-clock with last seq-cst write
      event->clock.UniteWith(lastSeqCstWrite->clock);
    }
  }


  // ===== Methods to create mo edges =====
  
  // Applies Seq-Cst Write-Read Coherence rules: establishes mo-edge between
  // last sc-write to the same location and `write` event
  // W'_x --sc--> R_x     W'_x --sc--> R_x
  //               ^       \            ^
  //               |        \           |
  //               rf  =>    \          rf
  //               |          \         |
  //              W_x          --mo--> W_x
  void CreateSeqCstReadWriteCoherenceEdges(Event* write, Event* read) {
    // TODO: in all such assertions do I need to check for correct RMW type?
    assert(write->IsWriteOrRMW() && read->IsReadOrRMW() && "Write and read events must be of correct type");
    assert(write->location == read->location && "Write and read events must be of the same location");
    assert(read->GetReadFromEvent() == write && "Read event must read-from the write event");

    EventId lastSeqCstWriteEvent = GetLastSeqCstWriteEventId(read->location);
    // no such event, no need to create mo edge
    if (lastSeqCstWriteEvent == -1) return;

    // create mo-edge between last sc-write and `write` that `read` event reads-from
    auto lastSeqCstWrite = events[lastSeqCstWriteEvent];    
    assert((lastSeqCstWrite->IsWrite() || lastSeqCstWrite->IsModifyRMW()) && "Last sc-write event must be a write or modifying rmw");
    assert(lastSeqCstWrite->location == read->location && "Last sc-write event must have the same location as read event");
    
    if (lastSeqCstWrite->id == write->id) return; // no need to create edge to itself
    
    // TODO: check that sc-edge between lastSeqCstWrite and read exists
    AddEdge(EdgeType::MO, lastSeqCstWrite->id, write->id);
  }

  // Applies Write-Read Coherence rules: establishes mo-edges between
  // stores (which happened-before `read`) and `write`
  // W'_x --hb--> R_x     W'_x --hb--> R_x
  //               ^       \            ^
  //               |        \           |
  //               rf  =>    \          rf
  //               |          \         |
  //              W_x          --mo--> W_x
  void CreateWriteReadCoherenceEdges(Event* write, Event* read) {
    assert(write->IsWriteOrRMW() && read->IsReadOrRMW() && "Write and read events must be of correct type");
    assert(write->location == read->location && "Write and read events must be of the same location");
    assert(read->GetReadFromEvent() == write && "Read event must read-from the write event");

    IterateThroughMostRecentEventsByPredicate(
      [write, read](Event* otherEvent) -> bool {
        return (
          read->id != otherEvent->id && write->id != otherEvent->id && // not the same events
          (otherEvent->IsWrite() || otherEvent->IsModifyRMW()) &&
          otherEvent->location == read->location &&
          otherEvent->HappensBefore(read)
        );
      },
      [this, write](Event* otherEvent) -> void {
        // establish mo edge
        AddEdge(EdgeType::MO, otherEvent->id, write->id);
      }
    );
  }

  // Applies Read-Read Coherence rules: establishes mo-edges between
  // `write` and write events from which other read events
  // (which happened-before `read`) reaf-from.
  // W'_x --rf--> R'_x     W'_x --rf--> R'_x
  //               |         |           |
  //               hb  =>    mo          hb
  //               |         |           |
  //               v         v           v
  // W_x  --rf--> R_x      W_x  --rf--> R_x
  void CreateReadReadCoherenceEdges(Event* write, Event* read) {
    assert(write->IsWriteOrRMW() && read->IsReadOrRMW() && "Write and read events must be of correct type");
    assert(write->location == read->location && "Write and read events must be of the same location");
    assert(read->GetReadFromEvent() == write && "Read event must read-from the write event");

    IterateThroughMostRecentEventsByPredicate(
      [write, read](Event* otherEvent) -> bool {
        return (
          read->id != otherEvent->id && write->id != otherEvent->id && // not the same events
          otherEvent->IsReadOrRMW() &&
          otherEvent->location == read->location &&
          otherEvent->HappensBefore(read) &&
          otherEvent->GetReadFromEvent() != nullptr && otherEvent->GetReadFromEvent() != write // R'_x does not read-from `write`
        );
      },
      [this, write](Event* otherRead) -> void {
        auto otherWrite = otherRead->GetReadFromEvent();
        // establish mo-edge
        AddEdge(EdgeType::MO, otherWrite->id, write->id);
      }
    );
  }

  // TODO: instead of sc-edges, add reads-from from "Repairing Sequential Consistency in C/C++11"?
  // Applies Seq-Cst / MO Consistency rules: establishes mo-edge between
  // last sc-write and current sc-write-event if their locations match
  // W'_x --sc--> W_x  =>  W'_x --mo--> W_x
  void CreateSeqCstConsistencyEdges(Event* event) {
    assert(event->IsWriteOrRMW() && event->IsSeqCst() && "Event must be a write/rmw with seq-cst order");

    EventId lastSeqCstWriteEvent = GetLastSeqCstWriteEventId(event->location);
    if (lastSeqCstWriteEvent == -1) return;
    auto lastSeqCstWrite = events[lastSeqCstWriteEvent];
    assert((lastSeqCstWrite->IsWrite() || lastSeqCstWrite->IsModifyRMW()) && "Last sc-write event must be a write or modifying rmw");
    assert(lastSeqCstWrite->location == event->location && "Last sc-write event must have the same location as event");
    // TODO: check that sc-edge between lastSeqCstWrite and read exists
    AddEdge(EdgeType::MO, lastSeqCstWrite->id, event->id);
  }

  // Applies Write-Write Coherence rules: establishes mo-edges between
  // other writes that happened before `event` only for the same location
  // W'_x --hb--> W_x  =>  W'_x --mo--> W_x
  void CreateWriteWriteCoherenceEdges(Event* event) {
    assert(event->IsWriteOrRMW());

    IterateThroughMostRecentEventsByPredicate(
      [event](Event* otherEvent) -> bool {
        return (
          event->id != otherEvent->id && // not the same event
          (otherEvent->IsWrite() || otherEvent->IsModifyRMW()) &&
          otherEvent->location == event->location && // same location
          otherEvent->HappensBefore(event)
        );
      },
      [this, event](Event* otherEvent) -> void {
        // establish mo edge
        AddEdge(EdgeType::MO, otherEvent->id, event->id);
      }
    );
  }

  // Applies Read-Write Coherence rules: establishes mo-edges between
  // stores that are read-from by reads which happened-before our write `event`
  // W'_x --rf--> R'_x      W'_x --rf--> R'_x
  //               |         \            |
  //               hb   =>    \           hb
  //               |           \          |
  //               v            \         v
  //              W_x            --mo--> W_x
  void CreateReadWriteCoherenceEdges(Event* event) {
    assert(event->IsWriteOrRMW());

    IterateThroughMostRecentEventsByPredicate(
      [event](Event* otherEvent) -> bool {
        return (
          event->id != otherEvent->id && // not the same event
          otherEvent->IsReadOrRMW() &&
          otherEvent->location == event->location && // same location
          otherEvent->HappensBefore(event)
        );
      },
      [this, event](Event* otherEvent) -> void {
        assert(otherEvent->GetReadFromEvent() != nullptr && "Read event must have read-from event");
        auto writeEvent = otherEvent->GetReadFromEvent();
        // establish mo edge
        AddEdge(EdgeType::MO, writeEvent->id, event->id);
      }
    );
  }

  // Applies RMW Consitency rules: establishes mo-edge between
  // store that is read-from by the current RMW event and the RMW event itself.
  // W_x --rf--> RMW_x  =>   W_x --mo--> RMW_x
  void CreateRmwConsistencyEdges(Event* write, Event* rmw) {
    assert(write->IsWriteOrRMW() && rmw->IsModifyRMW() && "Write and RMW events must be of correct type");
    assert(write->location == rmw->location && "Write and RMW events must be of the same location");
    assert(rmw->GetReadFromEvent() == write && "RMW event must read-from the Write event");
    
    // establish mo-edge
    AddEdge(EdgeType::MO, write->id, rmw->id);
  }

  // Applies RMW Atomicity rules: establishes mo-edges between
  // RMW event and all writes that are mo-after `write` event
  // (from which RMW reads-from)
  // RMW_x <--rf-- W_x     RMW_x <--rf-- W_x
  //                |        \            |
  //                mo   =>   \           mo
  //                |          \          |
  //                v           \         v
  //              W'_x           --mo--> W'_x
  void CreateRmwAtomicityEdges(Event* write, Event* rmw) {
    assert(write->IsWriteOrRMW() && rmw->IsModifyRMW() && "Write and RMW events must be of correct type");
    assert(write->location == rmw->location && "Write and RMW events must be of the same location");
    assert(rmw->GetReadFromEvent() == write && "RMW event must read-from the Write event");

    for (EdgeId edgeId : write->edges) {
      Edge& edge = edges[edgeId];
      if (edge.type == EdgeType::MO && edge.to != rmw->id) {
        // establish mo-edge
        AddEdge(EdgeType::MO, rmw->id, edge.to);
      }
    }
  }


  // ===== Helper methods =====

  template<class Predicate, class Callback>
  requires 
    requires(Predicate p, Event* event) {
      { p(event) } -> std::same_as<bool>;
    } &&
    requires(Callback cb, Event* event) {
      { cb(event) } -> std::same_as<void>;
    }
  void IterateThroughMostRecentEventsByPredicate(Predicate&& predicate, Callback&& callback) {
    // iterate through each thread and find last write-event that hb `event`
    for (int t = 0; t < nThreads; ++t) {
      // iterate from most recent to earliest events in thread `t`
      const auto& threadEvents = eventsPerThread[t];
      for (auto it = threadEvents.rbegin(); it != threadEvents.rend(); ++it) {
        Event* otherEvent = events[*it];
        if (!std::forward<Predicate>(predicate)(otherEvent)) continue;
        // invoke `callback` on the most recent event in the thread `t`
        std::forward<Callback>(callback)(otherEvent);
        break; // no need to invoke `callback` with earlier events from this thread
      }
    }
  }

  EventId GetLastSeqCstWriteEventId(int location) const {
    if (lastSeqCstWriteEvents.contains(location)) {
      return lastSeqCstWriteEvents.at(location);
    }
    return -1;
  }

  void AddEdge(EdgeType type, EventId from, EventId to) {
    // for mo edges we might add duplicates, so we need to check that such mo-edge does not exist
    if (type == EdgeType::MO && ExistsEdge(type, from, to)) {
      // std::cout << "Edge already exists: " << events[from]->AsString() << " --" << WmmUtils::EdgeTypeToString(type) << "--> "
      //           << events[to]->AsString() << std::endl;
      return;
    }

    auto& from_edges = events[from]->edges;
    EdgeId eId = edges.size();
    Edge e = { eId, type, from, to };
    edges.push_back(e);
    from_edges.push_back(eId);

    if (inSnapshotMode) {
      snapshotEdges.insert(eId);
    }
  }

  bool ExistsEdge(EdgeType type, EventId from, EventId to) const {
    const auto& from_edges = events[from]->edges;
    auto it = std::ranges::find_if(from_edges, [this, from, to, type](EdgeId eId) {
      auto& edge = edges[eId];
      return edge.from == from && edge.to == to && edge.type == type;
    });
    return it != from_edges.end();
  }

  // Check execution graph for consistency createria:
  //  * modification order is acyclic
  bool IsConsistent() {
    // TODO: should consistency criteria be taken from paper "Repairing Sequential Consistency in C/C++11"?
    enum {
      NOT_VISITED = 0,
      IN_STACK = 1,
      VISITED = 2
    };
    std::vector<int> colors(events.size(), NOT_VISITED); // each event is colored 0 (not visited), 1 (entered), 2 (visited)
    std::vector<std::pair<Event*, bool /* already considered */>> stack;

    for (auto e : events) {
      assert(colors[e->id] != IN_STACK && "Should not be possible, invalid cycle detection");
      if (colors[e->id] == VISITED) continue;
      stack.push_back({ e, false });

      while (!stack.empty()) {
        auto [event, considred] = stack.back();
        EventId eventId = event->id;
        
        stack.pop_back();
        if (considred) {
          colors[eventId] = VISITED;
          continue;
        }
        stack.push_back({ event, true }); // next time we take it out, we do not traverse its edges

        for (auto edgeId : event->edges) {
          Edge& edge = edges[edgeId];
          if (edge.type != EdgeType::MO) continue;
          if (colors[edge.to] == NOT_VISITED) {
            stack.push_back({ events[edge.to], false });
            colors[edge.to] = IN_STACK;
          }
          else if (colors[edge.to] == IN_STACK) {
            // cycle detected
            return false;
          }
        }
      }
    }

    return true;
  }

  void StartSnapshot() {
    assert(!inSnapshotMode && "Snapshot started twice");
    inSnapshotMode = true;
  }

  void ApplySnapshot() {
    assert(inSnapshotMode && "Applying snapshot not in snapshot mode");
    inSnapshotMode = false;
    snapshotEdges.clear();
  }

  void DiscardSnapshot() {
    assert(inSnapshotMode && "Discarding snapshot not in snapshot mode");
    // clearing all added edges from the graph
    // TODO: make sure below 'note' is true
    // Note: all appended edges will be in the suffixes of all edges arrays
    // 1. removing from edges vector
    while (!edges.empty() && snapshotEdges.contains(edges.back().id)) {
      edges.pop_back();
    }
    
    // 2. removing from events edges
    for (auto event : events) {
      auto& eventEdges = event->edges;
      while (!eventEdges.empty() && snapshotEdges.contains(eventEdges.back())) {
        eventEdges.pop_back();
      }
    }

    // reset snapshot state
    inSnapshotMode = false;
    snapshotEdges.clear();
  }

  void Clean() {
    edges.clear();
    for (auto event : events) {
      delete event;
    }
    events.clear();
    eventsPerThread.clear();
    lastSeqCstWriteEvents.clear();
  }

  void InitThreads(int nThreads) {
    this->nThreads = nThreads;
    eventsPerThread.resize(nThreads);

    // insert dummy events (with all-zero hbClocks),
    // which will be the first event in each thread
    for (int t = 0; t < nThreads; ++t) {
      // TODO: DummyEvents are all ?seq-cst? (now rlx) writes, do I need to add proper ?sc?-egdes between them? For now I don't
      int eventId = events.size();
      auto dummyEvent = new DummyEvent(eventId, nThreads, t);
      events.push_back(dummyEvent);
      eventsPerThread[t].push_back(eventId);
    }
  }

  std::vector<Edge> edges;
  std::vector<Event*> events;
  std::vector<std::vector<EventId>> eventsPerThread;
  std::map<int /* location */, EventId> lastSeqCstWriteEvents;
  std::unordered_set<EdgeId> snapshotEdges; // edges that are part of the snapshot (which case be discarded or applied, which is usefull when adding rf-edge)
  std::mt19937 gen{std::random_device{}()}; // random number generator for randomized rf-edge selection
  bool inSnapshotMode = false;
  int nThreads = 0;
};

class ExecutionGraph {
public:
ExecutionGraph(const ExecutionGraph&) = delete;
  ExecutionGraph& operator=(const ExecutionGraph&) = delete;
  ExecutionGraph(ExecutionGraph&&) = delete;
  ExecutionGraph& operator=(ExecutionGraph&&) = delete;

  static ExecutionGraph& getInstance() {
    static ExecutionGraph instance; // Thread-safe in C++11 and later
    return instance;
  }

  // Empties graph events and sets new number of threads. 
  void Reset(int nThreads) {
    std::cout << "Reset Graph: threads=" << nThreads << std::endl;
    this->nThreads = nThreads;
    this->nextLocationId = 0;

    graph.Reset(nThreads);
    graph.Print(std::cout);
  }

  // When new location is constructed, it registers itself in the wmm-graph
  // in order to generate corresponding initialization event.
  template<class T>
  int RegisterLocation(T value) {
    int currentLocationId = nextLocationId++;
    std::cout << "Register location: loc-" << currentLocationId << ", init value=" << value << std::endl;
    graph.AddWriteEvent(currentLocationId, WmmUtils::INIT_THREAD_ID, MemoryOrder::SeqCst, value);
    
    graph.Print(std::cout);
    return currentLocationId;
  }

  template<class T>
  T Load(int location, int threadId, MemoryOrder order) {
    // TODO: if we now only do real atomics, then they should be stored in graph, I guess?
    std::cout << "Load: loc-" << location << ", thread=" << threadId << ", order=" << WmmUtils::OrderToString(order) << std::endl;
    T readValue = graph.AddReadEvent<T>(location, threadId, order);

    graph.Print(std::cout);
    return readValue;
  }

  template<class T>
  void Store(int location, int threadId, MemoryOrder order, T value) {
    std::cout << "Store: loc-" << location << ", thread=" << threadId << ", order=" << WmmUtils::OrderToString(order) << ", value=" << value << std::endl;
    graph.AddWriteEvent(location, threadId, order, value);
    
    graph.Print(std::cout);
  }

  // TODO: generalize to any other type of the RMW operation
  //       (which have different method signature from compare_and_set, e.g. fetch_add, etc.)
  template<class T>
  std::pair<bool, T> ReadModifyWrite(int location, int threadId, T* expected, T desired, MemoryOrder success, MemoryOrder failure) {
    std::cout << "RMW: loc-" << location << ", thread=" << threadId
              << ", expected=" << *expected << ", desired=" << desired
              << ", success=" << WmmUtils::OrderToString(success)
              << ", failure=" << WmmUtils::OrderToString(failure) << std::endl;
    auto rmwResult = graph.AddRMWEvent<T>(location, threadId, expected, desired, success, failure);
    graph.Print(std::cout);
    std::cout << "RMW result: " << (rmwResult.first ? "MODIFY" : "READ") << ", value=" << rmwResult.second << std::endl;
    return rmwResult;
  }

    void setReadStrategy(ReadStrategy strategy) {
        graph.setReadStrategy(strategy);
    }


private:
  ExecutionGraph() = default;
  ~ExecutionGraph() = default;

  int nThreads = 0;
  int nextLocationId = 0;
  Graph graph;

  ReadStrategy currentStrategy = ReadStrategy::Random;

  // TODO: here can add real atomic's name via clangpass
};