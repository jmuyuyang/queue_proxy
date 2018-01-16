package compressor

import (
	"bufio"
	"compress/gzip"
	"io"
	"os"
)

const FileExt = ".gz"

type GzipCompressor struct {
}

func (c *GzipCompressor) Compress(file string, removeOrigin bool) error {
	compressFile := file + FileExt
	r, err := os.OpenFile(file, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	w, err := os.OpenFile(compressFile, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	rf := bufio.NewReader(r)
	wf := bufio.NewWriter(w)
	err = gzipFile(rf, wf)
	if err != nil {
		w.Close()
		os.Remove(compressFile)
		return err
	}
	wf.Flush()
	w.Close()
	r.Close()
	if removeOrigin {
		err = os.Remove(file)
	}
	return err
}

func (c *GzipCompressor) Decompress(file string, removeOrigin bool) error {
	compressFile := file + FileExt
	_, err := os.Stat(compressFile)
	if os.IsNotExist(err) {
		//文件不存在
		return nil
	}
	r, err := os.OpenFile(compressFile, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	w, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	rf := bufio.NewReader(r)
	wf := bufio.NewWriter(w)
	err = ungzipFile(rf, wf)
	if err != nil {
		r.Close()
		os.Remove(file)
		return err
	}
	wf.Flush()
	w.Close()
	r.Close()
	if removeOrigin {
		err = os.Remove(compressFile)
	}
	return err
}

func ungzipFile(in io.Reader, out io.Writer) error {
	gzReader, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	p := make([]byte, 4096)
	for {
		n, err := gzReader.Read(p)
		if n > 0 {
			out.Write(p[0:n])
		}
		if err != nil {
			break
		}
	}
	return err
}

func gzipFile(in io.Reader, out io.Writer) error {
	gzWriter := gzip.NewWriter(out)
	p := make([]byte, 4096)
	var err error
	for {
		n, err := in.Read(p)
		if n > 0 {
			gzWriter.Write(p[0:n])
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		return err
	}
	gzWriter.Flush()
	gzWriter.Close()
	return err
}
