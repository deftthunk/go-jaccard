package main

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/c2h5oh/datasize"
	mapset "github.com/deckarep/golang-set"
	"github.com/ernestosuarez/itertools"
	ffmt "gopkg.in/ffmt.v1"
)

type args struct {
	targetPath       string
	jaccardThreshold float32
	tempFolder       string
}

// get user input
func input() *args {
	if len(os.Args) < 3 {
		fmt.Println("Usage:")
		fmt.Println("jaccard <target folder> <jaccard threshold> [temp folder]")
		os.Exit(1)
	}

	thresh, _ := strconv.ParseFloat(os.Args[2], 32)

	// use executable folder unless user specifies a path
	tmpPath, _ := os.Executable()
	tmpPath = filepath.Dir(tmpPath)
	if len(os.Args) == 4 {
		tmpPath = os.Args[3]
	}

	userArgs := &args{
		targetPath:       os.Args[1],
		jaccardThreshold: float32(thresh),
		tempFolder:       tmpPath,
	}

	return userArgs
}

// walk file tree
func FilePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

// calculate jaccard distance on two sets at a time
func jaccard(set1 mapset.Set, set2 mapset.Set) float32 {
	intersection := set1.Intersect(set2)
	union := set1.Union(set2)

	intersectionLength, unionLength := 0, 0
	for range intersection.Iter() {
		intersectionLength++
	}
	for range union.Iter() {
		unionLength++
	}

	return float32(intersectionLength) / float32(unionLength)
}

// return a set of strings
func getstrings(fullpath string) mapset.Set {
	fileStrings, _ := exec.Command("strings", fullpath).CombinedOutput()
	strArray := strings.Split(string(fileStrings), "\n")
	stringSet := mapset.NewSet()

	for _, s := range strArray {
		stringSet.Add(s)
	}

	return stringSet
}

// threaded function for processing jaccard distances and combinations
func process(filePaths []string, userArgs *args, numberOfFiles int, ch chan string) {
	fileFeatures := make(map[string]mapset.Set, numberOfFiles)

	// create a map of file paths / string arrays
	for _, path := range filePaths {
		features := getstrings(path)
		fileFeatures[path] = features
	}

	for fArray := range itertools.CombinationsStr(filePaths, 2) {
		jaccardIndex := jaccard(fileFeatures[fArray[0]], fileFeatures[fArray[1]])

		if jaccardIndex > userArgs.jaccardThreshold {
			fmt.Printf("%s -- %s :: %f\n", fArray[0], fArray[1], jaccardIndex)
		}
	}

	ch <- "done"
}

// wait for threads to return
func threadWait(ch chan []string, callerSignal chan bool, threadReturn chan []interface{}, size int) {
	returnContainer := make([]interface{}, size)
	itemCounter := 0

	for itemCounter < (size - 1) {
		ret := <-ch
		//fmt.Println("Thread finished")
		returnContainer[itemCounter] = ret
		callerSignal <- true // signal to caller when we see a thread return
		itemCounter++
	}

	threadReturn <- returnContainer
}

func generateCombinations(filePaths []string, size int, tmpDir string, ch chan []string) {
	compressData := new(bytes.Buffer)
	compressor, _ := flate.NewWriter(compressData, 1)
	tmpFile := make([]*os.File, 1)
	round := 0
	tmpFile[round], _ = ioutil.TempFile(tmpDir, "_jaccard")

	for fArray := range itertools.CombinationsStr(filePaths, 2) {
		fString := fmt.Sprintf("%s,%s\n", fArray[0], fArray[1])
		data := []byte(fString)
		compressor.Write(data)

		/**
		  check to see if the compressed file size is growing too large.
		  if so, wrap up and close the compression handle, write the file
		  out, and open a new file and compression handle to continue
		  draining Combinations()
		  **/
		if uint64(compressData.Len()) >= uint64(400*datasize.MB) {
			fmt.Println("Size: ", compressData.Len()/1024/1024)

			compressor.Close()
			tmpFile[round].Write(compressData.Bytes())
			tmpFile[round].Close()

			compressData = new(bytes.Buffer)
			compressor, _ = flate.NewWriter(compressData, 5)
			newTmp, _ := ioutil.TempFile(tmpDir, "_jaccard")
			tmpFile = append(tmpFile, newTmp)
			round++
		}
	}

	compressor.Close()
	tmpFile[round].Write(compressData.Bytes())
	tmpFile[round].Close()

	tmpFileNames := make([]string, len(tmpFile))
	for i, obj := range tmpFile {
		tmpFileNames[i] = obj.Name()
	}

	ch <- tmpFileNames
}

// main
func main() {
	threads := runtime.NumCPU()
	_ = runtime.GOMAXPROCS(threads)
	ch := make(chan []string, threads)
	callerSignal := make(chan bool, threads)
	threadReturn := make(chan []interface{}, 1)
	userArgs := input()
	allFilePaths, _ := FilePathWalkDir(userArgs.targetPath)

	block := threads * 2 // arbitrary value to decrease memory usage. higher == lower mem
	splitFiles := make([][]string, block)
	numberOfFiles := len(allFilePaths)

	split := int(math.Floor(float64(numberOfFiles / block)))

	// use block to generate all combinations of file pairs.
	// combinations will be stored to temporary files
	for i := 0; i <= (block - 1); i++ {
		start := i * split

		if i+1 == block {
			splitFiles[i] = allFilePaths[start:]
			break
		}

		end := start + split
		splitFiles[i] = allFilePaths[start:end]
	}
	fmt.Printf("Split: %d, splitFiles Len: %d\n", split, len(splitFiles))

	/**
	  we're splitting up the files (splitFiles) due to memory constraints, but
	  we still have to compare every single file all the others, despite multiple
	  isolated threads.

	  here we generate all the index combinations of block, which will be
	  identical to the number of indexed sub-arrays in splitFiles. index
	  combos are stored in 'listCombos', and then we'll pass these combinations
	  to the threads
	  **/
	var listCombos [][]int
	for listIndexPair := range itertools.GenCombinations(block, 2) {
		listCombos = append(listCombos, listIndexPair)
	}
	//ffmt.Print(listCombos)
	fmt.Println("Combos: ", len(listCombos))

	// setup thread handler with the number of tasks (len(listCombos))
	comboCount := len(listCombos)

	/**
	  this *should* just make a new slice that points to a larger
	  section of the original array?
	  **/
	for cnt, combo := range listCombos {
		ffmt.Println("cnt: ", cnt, "combo: ", combo)
		comboSlice := make([]string, len(splitFiles[combo[0]]))
		//copy(comboSlice, splitFiles[combo[0]])
		comboSlice = append(comboSlice, splitFiles[combo[1]]...)
		//ffmt.Print("comboSlice", comboSlice)

		if cnt < threads {
			go generateCombinations(comboSlice, split, userArgs.tempFolder, ch)
			//fmt.Printf("Thread %d away!\n", cnt)
		} else if cnt == threads {
			go threadWait(ch, callerSignal, threadReturn, comboCount)
			//fmt.Println("kicked off threadWait")
		} else if <-callerSignal {
			//fmt.Println("Sending task ", cnt)
			go generateCombinations(comboSlice, split, userArgs.tempFolder, ch)
		} else {
			fmt.Println("stuck in 'else' land")
		}
	}

	//tempFileNames := <-threadReturn
	//fmt.Println(tempFileNames)

	/**
	  split up the work so we can use go block. we roughly (floor) divide
	  the number of files by block available, take equally sized slices of
	  file paths and store them in taskFiles, and make sure the last dump
	  gets everything else
	  **/

	/**
	    for i := 0; i <= (block-1); i++ {
	        start := i * split

	        if i + 1 == block {
	            taskFiles[i] = allFilePaths[start:]
	            break
	        }

	        end := start + split
	        taskFiles[i] = allFilePaths[start:end]

	        go process(taskFiles[i], userArgs, numberOfFiles, ch)
	    }
	**/

}
