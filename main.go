package main

import (
	"encoding/csv"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-gota/gota/dataframe"
	"github.com/go-gota/gota/series"
)

func main() {
	flagSrc := flag.String("path", "", "Source Path")
	flagDest := flag.String("dest", "", "Dest Path")
	flagDelim := flag.String("delim", ";", "Delim")
	flag.Parse()
	pathName := *flagSrc
	dl := strings.TrimSpace(*flagDelim)
	delim := []rune(dl)[0]
	pathDest := *flagDest

	if pathName == "" {
		log.Fatalf("No Source Path or Object Level Provided")
	}

	if pathDest == "" {
		pathDest = pathName
	}

	pathName = strings.Replace(pathName, `\`, `/`, -1)

	files, err := ioutil.ReadDir(pathName)
	if err != nil {
		log.Fatalf(`Error %s`, err.Error())
	}

	var wg sync.WaitGroup
	for _, f := range files {
		if !strings.Contains(f.Name(), "EDITED") && !strings.Contains(strings.ToLower(f.Name()), ".rar") && !strings.Contains(strings.ToLower(f.Name()), ".zip") && strings.Contains(strings.ToLower(f.Name()), ".csv") {
			wg.Add(1)
			go ProcessFile(f, pathName, pathDest, delim, &wg)
		}
	}

	wg.Wait()

}

func fileNameWithoutExtSliceNotation(fileName string) string {
	return fileName[:len(fileName)-len(filepath.Ext(fileName))]
}

func ProcessFile(f os.FileInfo, pathName, pathDest string, delim rune, wg *sync.WaitGroup) {
	defer wg.Done()
	csvFile, err := os.Open(filepath.Join(pathName, f.Name()))
	if err != nil {
		log.Fatal()
	}
	orFileName := f.Name()
	start := time.Now()

	defer csvFile.Close()
	df := dataframe.ReadCSV(csvFile, dataframe.WithDelimiter(delim))

	newCol := []string{}
	for i := 0; i < len(df.Names()); i++ {
		colName := df.Names()[i]
		if !strings.EqualFold(colName, "MRBTS/SBTS ID") &&
			!strings.EqualFold(colName, "LNBTS ID") &&
			!strings.EqualFold(colName, "MRBTS ID") &&
			!strings.EqualFold(colName, "SBTS ID") &&
			!strings.EqualFold(colName, "LNCEL ID") &&
			!strings.EqualFold(colName, "BSC ID") &&
			!strings.EqualFold(colName, "BCF ID") &&
			!strings.EqualFold(colName, "BTS ID") &&
			!strings.EqualFold(colName, "PLMN NAME") &&
			!strings.EqualFold(colName, "PLMN ID") &&
			!strings.EqualFold(colName, "WBTS ID") &&
			!strings.EqualFold(colName, "RNC ID") &&
			!strings.EqualFold(colName, "WCEL ID") &&
			!strings.EqualFold(colName, "DN") {

			newCol = append(newCol, colName)
		}
	}

	var nf dataframe.DataFrame
	newSelection := df.Select(newCol)

	nf = newSelection.Capply(CleanUP)

	newFileName := filepath.Join(pathDest, fileNameWithoutExtSliceNotation(f.Name())+"_EDITED.csv")

	fn, err := os.Create(newFileName)
	if err != nil {
		log.Fatalln(err.Error())
	}

	writer := csv.NewWriter(fn)
	writer.Comma = delim
	writer.WriteAll(nf.Records())
	writer.Flush()

	log.Printf("%s took %v\n", orFileName, time.Since(start))

}

func CleanUP(s series.Series) series.Series {

	stringRow := s.Records()
	newString := []string{}
	for _, r := range stringRow {
		x := strings.Index(r, "(id:")
		if r == "NaN" {
			r = ""
		}
		if x > -1 {
			newString = append(newString, strings.TrimSpace(r[:x]))
		} else {
			newString = append(newString, strings.TrimSpace(r))
		}
	}

	return series.Strings(newString)

}
