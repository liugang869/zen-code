
// For compatibility.

#define WORKLOAD YCSB
#define WARMUP 200000
#define MAX_TXN_PER_PART 200000
#define INIT_PARALLELISM 2
#define MAX_TUPLE_SIZE 1000
#define SYNTH_TABLE_SIZE 10000000
#define REQ_PER_QUERY 1
#define READ_PERC 0.5
#define WRITE_PERC 0.5
#define SCAN_PERC 0
#define ZIPF_THETA 0.99
#define THREAD_CNT 28
#define PART_CNT 1
#define IDX_HASH 1
#define IDX_MICA 2

#define CC_ALG MICA
#define ISOLATION_LEVEL SERIALIZABLE
#define VALIDATION_LOCK "no-wait"
#define PRE_ABORT "true"
#define RCU_ALLOC false
#define RCU_ALLOC_SIZE 20401094656UL

#define MICA_COLUMN_COUNT 1

#define INDEX_STRUCT IDX_MICA
#define MICA_FULLINDEX false

#define MICA_USE_SCAN false
#define MICA_USE_FULL_TABLE_SCAN false
#define MICA_MAX_SCAN_LEN 100

#define MICA_NO_TSC false
#define MICA_NO_PRE_VALIDATION false
#define MICA_NO_INSERT_NEWEST_VERSION_ONLY false
#define MICA_NO_SORT_WRITE_SET_BY_CONTENTION false
#define MICA_NO_STRAGGLER_AVOIDANCE false
#define MICA_NO_WAIT_FOR_PENDING false
#define MICA_NO_INLINING false
#define MICA_NO_BACKOFF false

#define MICA_USE_FIXED_BACKOFF false
#define MICA_FIXED_BACKOFF 0.

#define MICA_USE_SLOW_GC false
#define MICA_SLOW_GC 10

template <class StaticConfig>
class VerificationLogger;

struct DBConfig : public ::mica::transaction::BasicDBConfig {
// static constexpr bool kVerbose = true;

#if MICA_NO_PRE_VALIDATION
  static constexpr bool kPreValidation = false;
#endif
#if MICA_NO_INSERT_NEWEST_VERSION_ONLY
  static constexpr bool kInsertNewestVersionOnly = false;
#endif
#if MICA_NO_SORT_WRITE_SET_BY_CONTENTION
  static constexpr bool kSortWriteSetByContention = false;
#endif
#if MICA_NO_STRAGGLER_AVOIDANCE
  static constexpr bool kStragglerAvoidance = false;
#endif
#if MICA_NO_WAIT_FOR_PENDING
  static constexpr bool kNoWaitForPending = true;
#endif
#if MICA_NO_INLINING
  static constexpr bool kInlinedRowVersion = false;
#endif

#if MICA_NO_BACKOFF
  static constexpr bool kBackoff = false;
#endif

// static constexpr bool kPrintBackoff = true;
// static constexpr bool kPairwiseSleeping = true;

#if MICA_USE_FIXED_BACKOFF
  static constexpr double kBackoffMin = MICA_FIXED_BACKOFF;
  static constexpr double kBackoffMax = MICA_FIXED_BACKOFF;
#endif

#if MICA_USE_SLOW_GC
  static constexpr int64_t kMinQuiescenceInterval = MICA_SLOW_GC;
#endif

// typedef ::mica::transaction::WideTimestamp Timestamp;
// typedef ::mica::transaction::WideConcurrentTimestamp ConcurrentTimestamp;
#if MICA_NO_TSC
  typedef ::mica::transaction::CentralizedTimestamp Timestamp;
  typedef ::mica::transaction::CentralizedConcurrentTimestamp
      ConcurrentTimestamp;
#endif

  // static constexpr bool kCollectCommitStats = false;
  // static constexpr bool kCollectProcessingStats = true;
  // typedef ::mica::transaction::ActiveTiming Timing;

  // Switch this for verification.
  typedef ::mica::transaction::LoggerInterface<DBConfig> LoggerInterface;
  typedef ::mica::transaction::NullLogger<DBConfig> Logger;
  typedef ::mica::transaction::AepLogger<DBConfig> AepLogger;

  // typedef VerificationLogger<DBConfig> Logger;
};

// Debugging
static constexpr bool kVerbose = DBConfig::kVerbose;
static constexpr bool kShowPoolStats = true;
// static constexpr bool kShowPoolStats = false;
static constexpr bool kRunPerf = false;
// static constexpr bool kRunPerf = true;

// Workload generation.
// static constexpr bool kReadModifyWriteRatio = 0.0;
// static constexpr bool kReadModifyWriteRatio = 0.5;
static constexpr bool kReadModifyWriteRatio = 1.0;

#if 1

// HashIndex
#if INDEX_STRUCT == IDX_MICA
static constexpr bool kUseHashIndex = true;
#else
static constexpr bool kUseHashIndex = false;
#endif
static constexpr bool kUseBTreeIndex = false;

#else

// BTreeIndex
static constexpr bool kUseHashIndex = false;
#if INDEX_STRUCT == IDX_MICA
static constexpr bool kUseBTreeIndex = true;
#else
static constexpr bool kUseBTreeIndex = false;
#endif

#endif

#if MICA_FULLINDEX
static constexpr bool kSkipValidationForIndexAccess = false;
#else
static constexpr bool kSkipValidationForIndexAccess = true;
#endif

// static constexpr bool kUseSnapshot = false;
static constexpr bool kUseSnapshot = true;

static constexpr bool kUseContendedSet = false;
// static constexpr bool kUseContendedSet = true;
static constexpr uint64_t kContendedSetSize = 64;
static constexpr uint64_t kContendedReqPerTX = 2;
// static constexpr uint64_t kContendedSetSize = 1048576;
// static constexpr uint64_t kContendedReqPerTX = 1;
// static constexpr uint64_t kContendedSetSize = 10;
// static constexpr uint64_t kContendedReqPerTX = 1;

// static constexpr uint64_t kDataSize = 8;
// static constexpr uint64_t kColumnSize = 8;
// static constexpr uint64_t kDataSize = 10;
// static constexpr uint64_t kColumnSize = 10;
// static constexpr uint64_t kDataSize = 16;
// static constexpr uint64_t kColumnSize = 16;
// static constexpr uint64_t kDataSize = 100;
// static constexpr uint64_t kColumnSize = 100;
// static constexpr uint64_t kDataSize = 1000;
// static constexpr uint64_t kColumnSize = 100;
// static constexpr uint64_t kDataSize = 1000;
// static constexpr uint64_t kColumnSize = 1000;

static uint64_t kDataSize = MAX_TUPLE_SIZE;
static uint64_t kColumnSize = kDataSize / MICA_COLUMN_COUNT;

#if !MICA_USE_SCAN
static constexpr bool kUseScan = false;
#else
static constexpr bool kUseScan = true;
#endif
static constexpr uint32_t kMaxScanLen = MICA_MAX_SCAN_LEN;

#if !MICA_USE_FULL_TABLE_SCAN
static constexpr bool kUseFullTableScan = false;
#else
static constexpr bool kUseFullTableScan = true;
#endif

static constexpr int kNumaInUse = 0;
static constexpr uint64_t kPreMallocPow2 = (37UL);
static constexpr uint64_t kAepPreMallocPow2 = (39UL);

// static constexpr uint64_t kPreMallocSize = (1UL<<kPreMallocPow2);
// static constexpr uint64_t kAepPreMallocSize = (1UL<<kAepPreMallocPow2);

static uint64_t kPreMallocSize =    (16UL<<30);
static uint64_t kAepPreMallocSize = (64UL<<30);
// static constexpr uint64_t kAepPreMallocSize = (650UL<<30);

#if defined (ZEN)
static uint64_t kZenCacheSize = (1UL<<30);
#endif

#if defined (RECOVERY)
// when left 0.0825 of total transaction
static constexpr uint64_t kRecoveryPowSignal = (4);
static constexpr uint64_t kRecoverySignal = (1UL<<(kRecoveryPowSignal));
#endif
static constexpr uint64_t kRecoveryPowThread = (4);
static constexpr uint64_t kRecoveryThread = (1UL<<(kRecoveryPowThread));

/*
//====== TPCC workload =====
static uint64_t g_num_wh = 8;
static uint64_t g_thread_cnt = 8;

static constexpr uint64_t g_district_per_wearehouse = (10);
static constexpr uint64_t g_customer_per_district = (3000);
static constexpr uint64_t g_max_items = (100000);
static constexpr uint64_t g_average_items_per_order = (10);
static constexpr uint64_t g_max_orderline = (1UL<<32);

#define LASTNAME_LEN     (16)
#define FIRSTNAME_LEN    (16)
#define FIRSTNAME_MINLEN (8)
#define FIRST_PART_LOCAL (true)
#define TPCC_FULL        (true)
static constexpr enum RC { RCOK, Commit, Abort, WAIT, ERROR, FINISH }
#define TPCC_INSERT_ROWS  (true)
#define TPCC_INSERT_INDEX (true)
#define TPCC_CAVALIA_NO_OL_UPDATE (true)
static constexpr uint64_t kHaveToAbort = static_cast<uint64_t>(-1);
*/
