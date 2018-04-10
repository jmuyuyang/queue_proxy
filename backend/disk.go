package backend

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/jmuyuyang/queue_proxy/compressor"
	"github.com/jmuyuyang/queue_proxy/config"
	"github.com/jmuyuyang/queue_proxy/util"
)

const MaxBytesPerFile = 500 * 1024

type Compressor interface {
	Compress(string, bool) error
	Decompress(string, bool) error
}

type DiskQueue struct {
	writeChan       chan []byte
	readChan        chan []byte
	exitChan        chan int
	exitSyncChan    chan int
	readPos         int64
	nextReadPos     int64
	writePos        int64
	writeBuf        bytes.Buffer
	readFile        *os.File
	writeFile       *os.File
	writeFileNum    int64
	readFileNum     int64
	nextReadFileNum int64
	maxBytesPerFile int64
	reader          *bufio.Reader
	name            string
	dataPath        string
	syncTimeout     time.Duration
	needSync        bool
	compressor      Compressor
	logf            util.LoggerFuncHandler
}

func NewDiskQueue(cfg config.DiskConfig) (*DiskQueue, error) {
	d := DiskQueue{
		writeChan:       make(chan []byte),
		readChan:        make(chan []byte),
		exitChan:        make(chan int),
		exitSyncChan:    make(chan int),
		readPos:         0,
		nextReadPos:     0,
		writePos:        0,
		maxBytesPerFile: MaxBytesPerFile,
		dataPath:        cfg.Path,
		syncTimeout:     time.Duration(cfg.FlushTimeout) * time.Second,
		needSync:        false,
	}
	switch cfg.CompressType {
	case "gzip":
		d.compressor = &compressor.GzipCompressor{}
	}
	err := d.createDataPath()
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *DiskQueue) SetTopic(topicName string) {
	d.name = topicName
}

func (d *DiskQueue) SetLogger(logger util.LoggerFuncHandler) {
	d.logf = logger
}

func (d *DiskQueue) Start() error {
	//恢复元数据
	err := d.retrieveMetaData()
	if err != nil {
		return err
	}
	go d.ioLoop()
	return nil
}

func (d *DiskQueue) Stop() {
	close(d.exitChan)
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
}

func (d *DiskQueue) SendMessage(data []byte) error {
	d.writeChan <- data
	return nil
}

func (d *DiskQueue) GetMessageChan() chan []byte {
	return d.readChan
}

func (d *DiskQueue) createDataPath() error {
	_, err := os.Stat(d.dataPath)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		return os.Mkdir(d.dataPath, 0755)
	}
	return err
}

func (d *DiskQueue) ioLoop() {
	var err error
	var dataRead []byte
	var r chan []byte
	syncTicker := time.NewTicker(d.syncTimeout)

	for {

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(util.ErrorLvl, err.Error())
			}
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(util.ErrorLvl, err.Error())
					if os.IsNotExist(err) {
						//文件不存在,则递增需要读取的文件
						d.readFileNum++
						d.readPos = 0
						d.nextReadFileNum = d.readFileNum
						d.nextReadPos = d.readPos
					}
					continue
				}
				r = d.readChan
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		case r <- dataRead:
			d.moveForward()
		case dataWrite := <-d.writeChan:
			d.writeOne(dataWrite)
		case <-syncTicker.C:
			d.needSync = true
		case <-d.exitChan:
			//退出是进行一次meta data同步
			d.sync()
			goto exit
		}
	}

exit:
	syncTicker.Stop()
	d.exitSyncChan <- 1
}

func (d *DiskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			if os.IsNotExist(err) {
				//如果readFile文件不存在,则尝试一次解压缩
				if d.compressor != nil {
					err = d.compressor.Decompress(curFileName, true)
					if err == nil {
						d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
					}
				}
			}
			return nil, err
		}

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}
		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	return readBuf, nil
}

func (d *DiskQueue) moveForward() {
	d.readPos = d.nextReadPos
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum

	if oldReadFileNum != d.nextReadFileNum {
		d.needSync = true
		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(util.ErrorLvl, err.Error())
		}

		if d.compressor != nil && d.readFileNum+1 < d.writeFileNum {
			//写一个文件不是当前写文件则尝试进行解压缩
			go func(file string) {
				err := d.compressor.Decompress(file, true)
				if err != nil {
					d.logf(util.ErrorLvl, err.Error())
				}
			}(d.fileName(d.readFileNum + 1))
		}
	}
}

func (d *DiskQueue) writeOne(data []byte) error {
	var err error
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		} else {
			d.writeFile.Truncate(0)
		}
	}
	dataLen := int32(len(data))

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}
	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0
		err = d.sync()
		if err != nil {
			d.logf(util.ErrorLvl, err.Error())
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}

		if d.compressor != nil && d.writeFileNum-d.readFileNum > 2 {
			//已写完毕文件数比读文件数大于2则进行文件压缩
			go func(file string) {
				err := d.compressor.Compress(file, true)
				if err != nil {
					d.logf(util.ErrorLvl, err.Error())
				}
			}(d.fileName(d.writeFileNum - 1))
		}
	}
	return err
}

func (d *DiskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}
	d.needSync = false
	return nil
}

/**
* 恢复元数据信息
 */
func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "read_file:%d;read_pos:%d;,write_file:%d;write_pos:%d\n",
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%s:%d;%s:%d;,%s:%d;%s:%d\n",
		"read_file", d.readFileNum,
		"read_pos", d.readPos,
		"write_file", d.writeFileNum,
		"write_pos", d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s-diskqueue.meta.dat"), d.name)
}

func (d *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s-diskqueue-%06d.dat"), d.name, fileNum)
}
