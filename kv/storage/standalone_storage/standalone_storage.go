package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvPath, raftPath := conf.DBPath+"/kv", conf.DBPath+"/raft"
	kvEngine := engine_util.CreateDB(kvPath, false)
	var raftEngine *badger.DB
	if conf.Raft {
		raftEngine = engine_util.CreateDB(raftPath, true)
	}

	engines := engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath)
	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// 没有start功能
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Close()

}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false) //update参数作用未知
	return &StandAloneReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, item := range batch {
		switch item.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.engines.Kv, item.Cf(), item.Key(), item.Value())
			if err != nil {
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.engines.Kv, item.Cf(), item.Key())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
