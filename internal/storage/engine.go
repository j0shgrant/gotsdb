package storage

import (
	"errors"
	"fmt"
	"go.uber.org/zap"
)

type Engine struct {
	cold                 *ColdStorageService
	hot                  *HotStorageService
	autoCreateCollection bool
}

func NewEngine(dataDir string, autoCreate bool) (*Engine, error) {
	// create backing ColdStorageService
	cold, err := NewColdStorageService(dataDir)
	if err != nil {
		return nil, err
	}

	// create backing HotStorageService
	hot, err := NewHotStorageService()
	if err != nil {
		return nil, err
	}

	// create and return StorageEngine
	svc := &Engine{
		cold:                 cold,
		hot:                  hot,
		autoCreateCollection: autoCreate,
	}

	return svc, nil
}

// list all distinct collections across both hot and cold storage
func (e *Engine) ListCollections() ([]string, error) {
	hotCollections := e.hot.ListCollections()
	coldCollections, err := e.cold.ListCollections()
	if err != nil {
		return nil, err
	}

	// merge collection lists into set of distinct collection ids
	ids := make(map[string]bool)
	for _, id := range hotCollections {
		if _, exists := ids[id]; !exists {
			ids[id] = true
		}
	}
	for _, id := range coldCollections {
		if _, exists := ids[id]; !exists {
			ids[id] = true
		}
	}

	// build list of unique collection ids from map
	var uniqueIds []string
	for id := range ids {
		uniqueIds = append(uniqueIds, id)
	}

	return uniqueIds, nil
}

// check if a collection exists across hot and cold storage for a given collection id
func (e *Engine) CollectionExists(id string) (bool, error) {
	// first check if collection exists hot
	if e.hot.CollectionExists(id) {
		return true, nil
	}

	// if not, check if collection exists cold, as this is slower due to requiring filesystem io
	exists, err := e.cold.CollectionExists(id)
	if err != nil {
		return false, err
	}

	return exists, nil
}

// check if a collection is hot for a given collection id
func (e *Engine) IsHot(id string) (bool, error) {
	// check collection exists (hot or cold)
	exists, err := e.CollectionExists(id)
	if err != nil {
		return false, err
	}

	if exists {
		// check if collection is hot specifically
		return e.hot.CollectionExists(id), nil
	}

	return false, errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot or cold storage", id))
}

func (e *Engine) LoadCollection(id string) error {
	collection, err := e.cold.ReadFromDiskForId(id)
	if err != nil {
		return err
	}

	e.hot.collections[id] = collection

	return nil
}

func (e *Engine) FlushCollection(id string) error {
	// check that collection is hot
	if e.hot.CollectionExists(id) {
		// flush collection to disk
		err := e.cold.FlushToDisk(id, e.hot.collections[id])
		if err != nil {
			return err
		}

		// drop collection from hot storage
		return e.hot.DropCollection(id)
	}

	// return error if collection does not exist
	return errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot or cold storage", id))
}

func (e *Engine) FlushAllCollections() []error {
	var errs []error
	for id := range e.hot.collections {
		if err := e.FlushCollection(id); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func (e *Engine) ReadKey(id, key string) (string, error) {
	// read from hot storage if collection exists there
	if e.hot.CollectionExists(id) {
		value, err := e.hot.ReadKey(id, key)
		if err != nil {
			return "", err
		}

		return value, nil
	}

	// if collection does not exist in hot storage, check cold storage
	exists, err := e.cold.CollectionExists(id)
	if err != nil {
		return "", err
	}
	if exists {
		// load collection into hot storage
		err = e.LoadCollection(id)
		if err != nil {
			return "", err
		}

		// now read from hot storage
		value, err := e.hot.ReadKey(id, key)
		if err != nil {
			return "", err
		}

		return value, nil
	}

	// return "", err if collection does not exist in either hot or cold storage
	return "", errors.New(fmt.Sprintf("No collection found for collection id [%s] in hot or cold storage", id))
}

func (e *Engine) WriteKey(id, key, value string) error {
	// ensure collection exists hot, and return error if not
	err := e.LoadCollectionIfNotPresent(id)
	if err != nil {
		zap.S().Error(err)
		return err
	}

	// write key and pass any error encountered upward
	return e.hot.WriteKey(id, key, value)
}

func (e *Engine) LoadCollectionIfNotPresent(id string) error {
	// return nil if collection is already hot
	if e.hot.CollectionExists(id) {
		return nil
	}

	// check if collection exists cold, and load it if it does
	exists, err := e.cold.CollectionExists(id)
	if err != nil {
		return err
	}

	// if collection exists cold, load it into hot storage
	if exists {
		err = e.LoadCollection(id)
		if err != nil {
			return err
		}

		return nil
	}

	// if collection auto-creation is enabled, create collection hot
	if e.autoCreateCollection {
		zap.S().Infof("Creating collection [%s] in hot storage as it does not currently exist.", id)
		err = e.hot.CreateCollection(id)
		if err != nil {
			return err
		}

		return nil
	}

	// return error if collection is not present or newly created for given id in hot storage
	return errors.New(fmt.Sprintf("Unable to find a collection to load into hot storage with id [%s].", id))
}
