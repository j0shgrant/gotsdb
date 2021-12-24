package storage

import (
	"errors"
	"fmt"
)

type HotStorageService struct {
	collections map[string]Collection
}

func NewHotStorageService() (*HotStorageService, error) {
	svc := &HotStorageService{
		collections: make(map[string]Collection),
	}

	return svc, nil
}

func (svc *HotStorageService) ListCollections() []string {
	var ids []string
	for id := range svc.collections {
		ids = append(ids, id)
	}

	return ids
}

func (svc *HotStorageService) CollectionExists(id string) bool {
	_, exists := svc.collections[id]

	return exists
}

func (svc *HotStorageService) ReadKey(id, key string) (string, error) {
	if collection, exists := svc.collections[id]; exists {
		// return value for given key from collection given it exists
		if value, exists := collection[key]; exists {
			return value, nil
		}

		// return "", err if value does not exist for given key in collection
		return "", errors.New(fmt.Sprintf("No value found for key [%s] in hot collection [%s].", key, id))
	}

	// return "", err if collection does not exist
	return "", errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot storage.", id))
}

func (svc *HotStorageService) WriteKey(id, key, value string) error {
	if collection, exists := svc.collections[id]; exists {
		// write to collection if it exists
		collection[key] = value

		return nil
	}

	// return error if collection does not exist
	return errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot storage.", id))
}

func (svc *HotStorageService) CreateCollection(id string) error {
	// return error if collection already exists
	if svc.CollectionExists(id) {
		return errors.New(fmt.Sprintf("Collection already exists with id [%s] in hot storage.", id))
	}

	// create collection
	svc.collections[id] = Collection{}

	return nil
}

func (svc *HotStorageService) DropCollection(id string) error {
	// check if collection exists
	if _, exists := svc.collections[id]; exists {
		// drop collection
		delete(svc.collections, id)

		return nil
	}

	// return error if collection does not exist
	return errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot storage.", id))
}
