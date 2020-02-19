package units

const (
	_ = iota
	// KiB 1024 bytes
	KiB = 1 << (10 * iota)
	// MiB 1024 KiB
	MiB
	// GiB 1024 MiB
	GiB
	// TiB 1024 GiB
	TiB
	// PiB 1024 TiB
	PiB
)

const (
	// KB 1000 bytes
	KB = 1000
	// MB 1000 KB
	MB = KB * 1000
	// GB 1000 MB
	GB = MB * 1000
	// TB 1000 GB
	TB = GB * 1000
	// PB 1000 TB
	PB = TB * 1000
)

var (
	unitMap = map[string]int64{
		"B": 1,
		"b": 1,

		"KB": KB,
		"kb": KB,
		"MB": MB,
		"mb": MB,
		"GB": GB,
		"gb": GB,
		"TB": TB,
		"tb": TB,
		"PB": PB,
		"pb": PB,

		"K": KiB,
		"k": KiB,
		"M": MiB,
		"m": MiB,
		"G": GiB,
		"g": GiB,
		"T": TiB,
		"t": TiB,
		"P": PiB,
		"p": PiB,

		"KiB": KiB,
		"MiB": MiB,
		"GiB": GiB,
		"TiB": TiB,
		"PiB": PiB,
	}
)
