package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	key := req.GetKey()
	cf := req.GetCf()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	val, err2 := reader.GetCF(cf, key)
	if err2 != nil {
		return nil, err2
	}
	if val == nil {
		return &kvrpcpb.RawGetResponse{
			RegionError: nil,
			Error:       "",
			Value:       nil,
			NotFound:    true,
		}, nil
	}
	return &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       val,
		NotFound:    false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	key := req.GetKey()
	val := req.GetValue()
	cf := req.GetCf()
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Put{
			Key:   key,
			Value: val,
			Cf:    cf,
		}},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	key := req.GetKey()
	cf := req.GetCf()
	err := server.storage.Write(req.GetContext(), []storage.Modify{
		{Data: storage.Delete{
			Key: key,
			Cf:  cf,
		}},
	})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	cf := req.GetCf()
	limit := req.GetLimit()
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}
	it := reader.IterCF(cf)
	it.Seek(startKey)
	var kvs []*kvrpcpb.KvPair
	for i := 0; uint32(i) < limit; i++ {
		if !it.Valid() {
			break
		}
		item := it.Item()
		key := item.Key()
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: val,
		})
		it.Next()

	}

	return &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         kvs,
	}, nil
}
