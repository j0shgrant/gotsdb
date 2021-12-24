package storage

import (
	"encoding/gob"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type ColdStorageService struct {
	dataDir string
}

func NewColdStorageService(dataDir string) (*ColdStorageService, error) {
	// validate data directory path
	dataDirPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}

	// create data directory if it doesn't already exist
	zap.S().Infof("Checking if data directory already exists at [%s].", dataDirPath)
	if _, err := os.Stat(dataDirPath); errors.Is(err, os.ErrNotExist) {
		zap.S().Infof("Creating data directory at [%s].", dataDirPath)
		err = os.Mkdir(dataDirPath, os.ModePerm)
		if err != nil {
			zap.S().Errorf("Failed to create data directory at [%s].", dataDirPath)
			return nil, err
		}

		zap.S().Infof("Successfully created data directory at [%s].", dataDirPath)
	} else {
		zap.S().Infof("Existing data directory found at [%s].", dataDirPath)
	}


	zap.S().Infof("Launching storage service with data directory [%s].", dataDirPath)

	svc := &ColdStorageService{
		dataDir: dataDirPath,
	}

	return svc, nil
}

func (svc *ColdStorageService) ListCollections() ([]string, error) {
	// open data directory
	f, err := os.Open(svc.dataDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			zap.S().Error(err)
		}
	}()

	// list children for data directory
	children, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	// filter out directories and files without .tsdata file extension
	fileNames := make([]string, 0)
	for _, child := range children {
		if !child.IsDir() {
			match, err := regexp.MatchString("[a-zA-Z0-9]+.tsdata", child.Name())
			if err != nil {
				return nil, err
			}

			if match {
				fileNames = append(fileNames, strings.Split(child.Name(), ".tsdata")[0])
			}
		}
	}

	return fileNames, nil
}

func (svc *ColdStorageService) CollectionExists(id string) (bool, error) {
	// build absolute filepath for data file
	dataFilePath := filepath.Join(svc.dataDir, fmt.Sprintf("%s.tsdata", id))

	// check if file exists
	info, err := os.Stat(dataFilePath)
	if err != nil {
		// return false, nil if file doesn't exist
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		// return false, err if any other error has been encountered
		return false, err
	}

	if info.IsDir() {
		// return false, err is file is a directory
		return false, errors.New(fmt.Sprintf("File [%s] is a directory when it should be a normal file.", dataFilePath))
	}

	// return true, nil if file is a valid data file
	return true, nil
}

func (svc *ColdStorageService) ReadFromDiskForId(id string) (Collection, error) {
	zap.S().Info("Reading collection from disk.")

	// open data file
	f, err := os.OpenFile(filepath.Join(svc.dataDir, fmt.Sprintf("%s.tsdata", id)), os.O_RDONLY, 0755)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = f.Close()
		if err != nil {
			zap.S().Error(err)
		}
	}()

	// deserialise binary data file to return type
	var data Collection
	if err = gob.NewDecoder(f).Decode(&data); err != nil {
		zap.S().Errorf("error in reading data file for collection [%s]: %s", id, err.Error())
		return nil, err
	}

	return data, nil
}

func (svc *ColdStorageService) FlushToDisk(id string, data Collection) error {
	zap.S().Info("Flushing collection to disk.")

	// open data file
	f, err := os.OpenFile(filepath.Join(svc.dataDir, fmt.Sprintf("%s.tsdata", id)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer func() {
		err = f.Close()
		if err != nil {
			zap.S().Error(err)
		}
	}()

	// serialise data to binary and overwrite data file
	err = gob.NewEncoder(f).Encode(data)
	return err
}

// returns a handle for a file for a given id - REMEMBER TO CLOSE IT
func (svc *ColdStorageService) getDataFileForId(id string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(svc.dataDir, fmt.Sprintf("%s.tsdata", id)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return nil, err
	}

	return f, nil
}
