package constant

const Version = 1
// 256TB
const MaxMapSize = 0xFFFFFFFFFFFF
const MaxMmapStep = 1 << 30 // 1GB
// maxAllocSize is the size used when creating array pointers.
const MaxAllocSize = 0x7FFFFFFF