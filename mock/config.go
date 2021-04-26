package mock

type ConfigWatcher interface {
	OnNewConfig(cfg uint)
}
