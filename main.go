package main

import (
  "fmt"
  "encoding/csv"
  "os"
  "strconv"
  "gonum.org/v1/gonum/stat"
  "time"
  "strings"
  "log"
  "sync"
  "io"
)

type mapper func(tickerDatum) float32

func tickerListMapper(data []tickerDatum, theMap mapper) []float32 {
  var aggList []float32
  for _, datum := range data {
    aggList = append(aggList, theMap(datum))
  }
  return aggList
}



type tickerDatum struct {
  date      time.Time
  symbol    string
  volume    int
  open      float32
  close     float32
  high      float32
  low       float32
  adjclose  float32
}

type regResult struct {
  symbol string
  alpha  float64
  beta   float64
}


func (tik tickerDatum) diff() float32 {
  return tik.close - tik.open
}

func fromCSV(ln []string) tickerDatum {
  splitDate := strings.Split(ln[0], "-")
  yearNum, _ := strconv.Atoi(splitDate[0])
  monthNum, _ := strconv.Atoi(splitDate[1])
  dayNum, _ := strconv.Atoi(splitDate[2])
  date := time.Date(yearNum, time.Month(monthNum), dayNum, 0, 0, 0, 0, time.UTC)
  vol, _ := strconv.Atoi(ln[2])
  op, _ := strconv.ParseFloat(ln[3], 32)
  close, _ := strconv.ParseFloat(ln[4], 32)
  high, _ := strconv.ParseFloat(ln[5], 32)
  low, _ := strconv.ParseFloat(ln[6], 32)
  adjClose, _ := strconv.ParseFloat(ln[7], 32)
  datum := tickerDatum{
    date:      date,
    symbol:    ln[1],
    volume:    vol,
    open:      float32(op),
    close:     float32(close),
    high:      float32(high),
    low:       float32(low),
    adjclose:  float32(adjClose),
  }
  return datum
}

func groupByTicker(data []tickerDatum) [][]tickerDatum {
  var theList [][]tickerDatum
  var currentSymbol string = ""
  var currentList []tickerDatum
  for _, datum := range data  {
    if datum.symbol == currentSymbol {
      currentList = append(currentList, datum)
    } else {
      theList = append(theList, currentList)
      currentSymbol = datum.symbol
      currentList = make([]tickerDatum, 0)
    }
  }
  return theList
}

func groupChanByTicker(tickerDatumChan chan tickerDatum) chan []tickerDatum {
  outChan := make(chan []tickerDatum, 0)
  var currentSymbol string = ""
  var currentList []tickerDatum
  go func() {
    for datum := range tickerDatumChan {
      if datum.symbol == currentSymbol {
        currentList = append(currentList, datum)
      } else {
        if currentSymbol == "" {
          currentSymbol = datum.symbol
          currentList = append(currentList, datum)
        } else {
          outChan <- currentList
          currentSymbol = datum.symbol
          currentList = make([]tickerDatum, 0)
        }
      }
    }
    close(outChan)
  }()
  return outChan
}

func regress(dataIn <-chan []tickerDatum, xMapper, yMapper mapper) (<-chan regResult) {
  out := make(chan regResult, 0)
  go func() {
    for data := range dataIn {
      var xList []float64
      var yList []float64
      for _, datum := range data {
        xList = append(xList, float64(xMapper(datum)))
        yList = append(yList, float64(yMapper(datum)))
      }

      // Do not force the regression line to pass through the origin.
      origin := false

      alpha, beta := stat.LinearRegression(xList, yList, nil, origin)
      // stat.RSquared(days, opens, nil, alpha, betma)
      if len(data) > 0 {
        result := regResult{symbol: data[0].symbol, alpha: alpha, beta: beta}
        out <- result
      }
    }
    close(out)
  }()
  return out
}

func merge(cs ...<-chan regResult) <-chan regResult {
    var wg sync.WaitGroup
    out := make(chan regResult)
    // Start an output goroutine for each input channel in cs.  output
    // copies values from c to out until c is closed, then calls wg.Done.
    output := func(c <-chan regResult) {
        for n := range c {
            out <- n
        }
        wg.Done()
    }
    wg.Add(len(cs))
    for _, c := range cs {
        go output(c)
    }
    // Start a goroutine to close out once all the output goroutines are
    // done.  This must start after the wg.Add call.
    go func() {
        wg.Wait()
        close(out)
    }()
    return out
}

func fileReader(fileName string) chan []string {
  f, _ := os.Open(fileName)
  finData := csv.NewReader(f)
  finData.Read()
  lineChan := make(chan []string, 0)
  go func() {
    for {
      res, err := finData.Read()
      if err == io.EOF {
        close(lineChan)
        break
      }
      if err != nil {
        log.Fatal(err)
      }
      lineChan <- res
    }
  }()
  return lineChan
}

func makeTickerDatum(lines chan []string) chan tickerDatum {
  datumChan := make(chan tickerDatum, 0)
  go func() {
    for ln := range lines {
      splitDate := strings.Split(ln[0], "-")
      yearNum, _ := strconv.Atoi(splitDate[0])
      monthNum, _ := strconv.Atoi(splitDate[1])
      dayNum, _ := strconv.Atoi(splitDate[2])
      date := time.Date(yearNum, time.Month(monthNum), dayNum, 0, 0, 0, 0, time.UTC)
      vol, _ := strconv.Atoi(ln[2])
      op, _ := strconv.ParseFloat(ln[3], 32)
      close, _ := strconv.ParseFloat(ln[4], 32)
      high, _ := strconv.ParseFloat(ln[5], 32)
      low, _ := strconv.ParseFloat(ln[6], 32)
      adjClose, _ := strconv.ParseFloat(ln[7], 32)
      datum := tickerDatum{
        date:      date,
        symbol:    ln[1],
        volume:    vol,
        open:      float32(op),
        close:     float32(close),
        high:      float32(high),
        low:       float32(low),
        adjclose:  float32(adjClose),
      }
      datumChan <- datum
    }
    close(datumChan)
  }()
  return datumChan
}

func newMain() {
  start := time.Now()
  lineChan := fileReader("amex-nyse-nasdaq-stock-histories/history_60d.csv")
  tickerDatumChan := makeTickerDatum(lineChan)
  tickerGroupChan := groupChanByTicker(tickerDatumChan)
  regResultChan := regress(tickerGroupChan, func(t tickerDatum) float32 { return float32(t.date.Day())}, func(t tickerDatum) float32 { return t.open})
  regResultChan2 := regress(tickerGroupChan, func(t tickerDatum) float32 { return float32(t.date.Day())}, func(t tickerDatum) float32 { return t.open})
  regResultChan3 := regress(tickerGroupChan, func(t tickerDatum) float32 { return float32(t.date.Day())}, func(t tickerDatum) float32 { return t.open})
  regResultChan4 := regress(tickerGroupChan, func(t tickerDatum) float32 { return float32(t.date.Day())}, func(t tickerDatum) float32 { return t.open})
  regResultChan5 := regress(tickerGroupChan, func(t tickerDatum) float32 { return float32(t.date.Day())}, func(t tickerDatum) float32 { return t.open})
  finalResultChan := merge(regResultChan, regResultChan2, regResultChan3, regResultChan4, regResultChan5)
  for res := range finalResultChan {
    // fmt.Println(res.symbol)
    fmt.Println(res.alpha)
    // fmt.Println(res.beta)
  }
  elapsed := time.Since(start)
  log.Printf("Calculation took %s", elapsed)
}

func main() {
  newMain()
}
