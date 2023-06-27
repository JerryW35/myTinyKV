package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	cf, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	var ret kvrpcpb.RawGetResponse
	if cf != nil {
		ret = kvrpcpb.RawGetResponse{
			Value:    cf,
			NotFound: false,
		}
	} else {
		ret = kvrpcpb.RawGetResponse{
			NotFound: true,
		}
	}
	return &ret, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	cf, key, val := req.GetCf(), req.GetKey(), req.GetValue()
	data := storage.Put{
		Key:   key,
		Value: val,
		Cf:    cf,
	}
	modify := storage.Modify{Data: data}
	batch := []storage.Modify{modify}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	response := &kvrpcpb.RawPutResponse{
		//Error: err.Error(),
	}
	return response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	cf, key := req.GetCf(), req.GetKey()
	data := storage.Delete{
		Key: key,
		Cf:  cf,
	}
	modify := storage.Modify{Data: data}
	batch := []storage.Modify{modify}
	err := server.storage.Write(nil, batch)
	if err != nil {
		return nil, err
	}
	// not sure if response needed
	response := &kvrpcpb.RawDeleteResponse{
		//Error: err.Error(),
	}
	return response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	startKey := req.GetStartKey()
	limit := req.GetLimit()
	cf := req.GetCf()
	var response *kvrpcpb.RawScanResponse
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return response, err
	}
	iter := reader.IterCF(cf)

	var responseKvs []*kvrpcpb.KvPair
	for iter.Seek(startKey); iter.Valid(); iter.Next() {

		kvs := iter.Item()
		key := kvs.Key()
		val, _ := kvs.Value()
		responseKvs = append(responseKvs, &kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		})
		limit--
		if limit == 0 {
			break
		}

	}
	response = &kvrpcpb.RawScanResponse{
		Kvs: responseKvs,
	}
	return response, nil
}
