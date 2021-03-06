// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"bufio"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	//"github.com/pingcap/tidb/planner/core"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

var (
	_ SelectResult = (*selectResult)(nil)
	_ SelectResult = (*streamResult)(nil)
	_ SelectResult = (*csvSelectResult)(nil)
	_ SelectResult = (*pgSelectResult)(nil)
	_ SelectResult = (*redisSelectResult)(nil)
)

// SelectResult is an iterator of coprocessor partial results.
type SelectResult interface {
	// Fetch fetches partial results from client.
	Fetch(context.Context)
	// NextRaw gets the next raw result.
	NextRaw(context.Context) ([]byte, error)
	// Next reads the data into chunk.
	Next(context.Context, *chunk.Chunk) error
	// Close closes the iterator.
	Close() error
}

type resultWithErr struct {
	result kv.ResultSubset
	err    error
}

type row struct{
	cols []interface{}
}

type pgSelectResult struct{
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunck chan chunk.Chunk  //this is for data from csv
	dataRow chan row
	data chunk.Chunk //this is for data storage
	dataR row
	info []*model.ColumnInfo
	pushDownConditions string
	//plans []core.PhysicalPlan
}



func (r *pgSelectResult) fetch(){
	for{
		break
	}
}

func (r *pgSelectResult) Fetch(ctx context.Context) {
	go r.fetch()
}

func (r *pgSelectResult) NextRaw(context.Context) ([]byte, error) {
	log.Print("IN NEXTRAW")
	return nil,nil
}

func (r *pgSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error{
	chk.Reset()
	for {
		if r.over==true && len(r.dataRow)==0 {
			break
		}
		select{
		case r.over = <-r.isOver:{
			break
		}
		case r.dataR = <- r.dataRow :{
			for i:=0;i< len(r.info);i++ {
				switch r.info[i].Tp {
				case mysql.TypeLong:
					x,_ := r.dataR.cols[i].(int)
					chk.AppendInt64(i,int64(x))
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
					mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
					x,_ := r.dataR.cols[i].(string)
					chk.AppendString(i,x)
				}
				case mysql.TypeFloat:{
					x,_:=r.dataR.cols[i].(float64)
					chk.AppendFloat32(i,float32(x))
				}

				}
			}
			break
		}

		}

	}

	return nil
}

func (r *pgSelectResult) Close() error{
	return nil
}


type pgReader struct{
	path string
	isOver chan bool
	dataChunck chan chunk.Chunk
	dataRow chan row
	info []*model.ColumnInfo
	//schema
	//plans []core.PhysicalPlan
	pushDownConditions string
}

type RequestPG struct{
	SQL string
	Count int
}

type ResultPG struct{
	Result string
}

func (cR *pgReader) readPG(){
	pathinfos := strings.Split(cR.path,"#")
	address := pathinfos[0]+":"+pathinfos[1]
	tableName := pathinfos[2]
	client,err := rpc.DialHTTP("tcp",address)
	if err!=nil {
		log.Print(err)
	}

	//make the plan to sql
	/*
	TARGET LIST
	*/
	attr :=""
	for i,item:=range cR.info{
		if i>0 {
			attr+=","
		}
		attr+=item.Name.L
	}
	/*
	WHERE CONDITION
	*/
	log.Println(cR.pushDownConditions)

	conditionString := ""
	if cR.pushDownConditions!="" {
		conditionString += "where " + cR.pushDownConditions
	}
	/*
	conditionPlan := cR.plans[1]
	conditionPlan2,ok:=conditionPlan.(*core.PhysicalSelection)
	if ok {
		conditions:=conditionPlan2.Conditions
		for i,oneCondition:=range conditions{
			if i>0 {
				conditionString += "and "
			}
			//if it is a scalarfunction
			tryFunction,ok := oneCondition.(*expression.ScalarFunction)
			if ok {
				//tryFunction.FuncName.L
				//tryFunction.Function.
			}
		}
	}*/

	/*
	THE FINAL SQL
	*/
	sql := "select "+attr + " from " + tableName + " "+conditionString+";"
	log.Println(sql)
	args := &RequestPG{sql,len(cR.info)}
	var reply ResultPG
	err = client.Call("PGX.Require",args,&reply)
	if err!=nil {
		log.Println(err)
	}
	if reply.Result==""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result,",")
	for _,record:=range recordss  {
		records := strings.Split(record,"#")
		dataVal := make([]interface{},0)
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1,err1 := strconv.Atoi(string(records[i]))
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[i])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[i],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			}}
		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}


func GetPGSelectResult(path string,info []*model.ColumnInfo,pushDownConditions string)(SelectResult, error){
	dataRowChan := make(chan row,1)
	isOver := make(chan bool,1)
	cReader := pgReader{path:path,dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions}//plans:plans}
	go (&cReader).readPG()
	return &pgSelectResult{dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions},nil//plans:plans},nil
}

type RequestRedis struct{
	/*
	1)support key match:point and RE[1,2]
	2)value:point [3]
	3)the first version is just string forms
	4)no condition read all k{v}[4]
	*/
	QueryType int
	Offsets []int //0 for key and 1 for value
	Value string //this is prama eg:a="b" b is para
	Count int
}

type ResultRedis struct{
	Result string //the same as pg and csv
}

type redisSelectResult struct{
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunck chan chunk.Chunk  //this is for data from csv
	dataRow chan row
	data chunk.Chunk //this is for data storage
	dataR row
	info []*model.ColumnInfo
	pushDownConditions string
}

func (r *redisSelectResult) fetch(){
	for{


		break
	}
}

func (r *redisSelectResult) Fetch(ctx context.Context) {
	go r.fetch()
}

func (r *redisSelectResult) NextRaw(context.Context) ([]byte, error) {
	log.Print("IN NEXTRAW")
	return nil,nil
}

func (r *redisSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error{
	chk.Reset()
	for {
		if r.over==true && len(r.dataRow)==0 {
			break
		}
		select{
		case r.over = <-r.isOver:{
			break
		}
		case r.dataR = <- r.dataRow :{
			for i:=0;i< len(r.info);i++ {
				switch r.info[i].Tp {
				case mysql.TypeLong:
					x,_ := r.dataR.cols[i].(int)
					chk.AppendInt64(i,int64(x))
				case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
					mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
					x,_ := r.dataR.cols[i].(string)
					chk.AppendString(i,x)
				}
				case mysql.TypeFloat:{
					x,_:=r.dataR.cols[i].(float64)
					chk.AppendFloat32(i,float32(x))
				}

				}
			}
			break
		}

		}

	}

	return nil
}

func (r *redisSelectResult) Close() error{
	return nil
}


type redisReader struct{
	path string
	isOver chan bool
	dataChunck chan chunk.Chunk
	dataRow chan row
	info []*model.ColumnInfo
	pushDownConditions string
	//schema
}
func (cR *redisReader) readRedis(){
	pathinfos := strings.Split(cR.path,"#")
	address := pathinfos[0]+":"+pathinfos[1]
	//csvName := pathinfos[2] no need just to connect rpc
	client,err := rpc.DialHTTP("tcp",address)
	if err!=nil {
		log.Print(err)
	}
	offsets := make([]int,0)
	for i:=0;i<len(cR.info);i++  {
		offsets = append(offsets,cR.info[i].Offset)
	}
	//this part is commint from the pushDownCondtitions
	parax:=""
	queryType:=100
	if cR.pushDownConditions=="" {
		queryType=	4
	}else{
		log.Println("sss",cR.pushDownConditions)
		infos:=strings.Split(cR.pushDownConditions,"#")
		queryType ,_= strconv.Atoi(infos[0])
		if queryType==1 {
			parax= infos[1]
		}else if queryType==2{
			parax = infos[1]+"*"
		}
	}
	log.Println("Redis")
	args := &RequestRedis{queryType,offsets,parax,0}
	log.Println(args)
	var reply ResultRedis
	err = client.Call("RedisXX.Require",args,&reply)
	if err!=nil {
		log.Println(err)
	}
	if reply.Result==""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result,",")
	for _,record:=range recordss  {
		records := strings.Split(record,"#")
		dataVal := make([]interface{},0)
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1,err1 := strconv.Atoi(string(records[i]))
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[i])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[i],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			}}
		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}


//BY LANHAI:this function will be called to create a csvSR
func GetRedisSelectResult(path string,info []*model.ColumnInfo,pushDownConditions string)(SelectResult, error){
	dataRowChan := make(chan row,1)
	isOver := make(chan bool,1)
	redisReader := redisReader{path:path,dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions}
	go (&redisReader).readRedis()
	return &redisSelectResult{dataRow:dataRowChan,isOver:isOver,info:info,pushDownConditions:pushDownConditions},nil
}


type csvSelectResult struct{
	fieldTypes []*types.FieldType
	isOver chan bool
	over bool
	dataChunck chan chunk.Chunk  //this is for data from csv
	dataRow chan row
	data chunk.Chunk //this is for data storage
	dataR row
	info []*model.ColumnInfo
}

func (r *csvSelectResult) fetch(){
	for{


		break
	}
}

func (r *csvSelectResult) Fetch(ctx context.Context) {
	go r.fetch()
}

func (r *csvSelectResult) NextRaw(context.Context) ([]byte, error) {
	log.Print("IN NEXTRAW")
	return nil,nil
}

func (r *csvSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error{
	chk.Reset()
	for {
		if r.over==true && len(r.dataRow)==0 {
			break
		}
		select{
			case r.over = <-r.isOver:{
				break
			}
			case r.dataR = <- r.dataRow :{
				for i:=0;i< len(r.info);i++ {
					switch r.info[i].Tp {
					case mysql.TypeLong:
						x,_ := r.dataR.cols[i].(int)
						chk.AppendInt64(i,int64(x))
					case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
						mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
						x,_ := r.dataR.cols[i].(string)
						chk.AppendString(i,x)
					}
						case mysql.TypeFloat:{
							x,_:=r.dataR.cols[i].(float64)
							chk.AppendFloat32(i,float32(x))
						}

					}
				}
				break
			}

		}

	}

	return nil
}

func (r *csvSelectResult) Close() error{
	return nil
}

type csvReader struct{
	path string
	isOver chan bool
	dataChunck chan chunk.Chunk
	dataRow chan row
	info []*model.ColumnInfo
	//schema
}

type Requestx2 struct{
	Start int
	TableName string
	Offsets []int
}

type Resultx2 struct{
	Count int
	Result string
}

func (cR *csvReader) readFile2(){
	pathinfos := strings.Split(cR.path,"#")
	address := pathinfos[0]+":"+pathinfos[1]
	csvName := pathinfos[2]
	client,err := rpc.DialHTTP("tcp",address)
	if err!=nil {
		log.Print(err)
	}
	offsets := make([]int,0)
	for i:=0;i<len(cR.info);i++  {
		offsets = append(offsets,cR.info[i].Offset)
	}
	args := &Requestx2{0,csvName,offsets}
	var reply Resultx2
	err = client.Call("CSVX.Require",args,&reply)
	if err!=nil {
		log.Println(err)
	}
	if reply.Result==""{
		cR.isOver<-true
		return
	}
	recordss := strings.Split(reply.Result,",")
	for _,record:=range recordss  {
		records := strings.Split(record,"#")
		dataVal := make([]interface{},0)
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
			case mysql.TypeLong:{
				x1,err1 := strconv.Atoi(string(records[i]))
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[i])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[i],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
			}}
		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}

func (cR *csvReader) readFile(){
	//suppose we know the schema,later we need to store the schema,when init the csvReader
	f,err := os.OpenFile(cR.path,os.O_RDONLY,0644)
	if err != nil{
		log.Println("OPEN ERROR")
		return // there maybe some send some infos to let the csvSR know nothing this time
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for{
		recordTemp,_,err:=reader.ReadLine()
		if err!=nil {
			//log.Println("Read Over")
			break
		}
		dataVal := make([]interface{},0)
		recordTemp = recordTemp[:len(recordTemp)]
		recordTempS := string(recordTemp)
		records:=strings.Split(recordTempS,",")
		for i:=0;i<len(cR.info);i++  {
			switch cR.info[i].Tp {
				case mysql.TypeLong:{
					x1,err1 := strconv.Atoi(string(records[cR.info[i].Offset]))
					if  err1 != nil {
						log.Print(err1)
					}
					dataVal = append(dataVal,x1)
				}
			case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
				mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:{
				x1 := string(records[cR.info[i].Offset])
				dataVal = append(dataVal,x1)
			}
			case mysql.TypeFloat:{
				//v1, err := strconv.ParseFloat(v, 32)
				x1,err1 := strconv.ParseFloat(records[cR.info[i].Offset],32)
				if  err1 != nil {
					log.Print(err1)
				}
				dataVal = append(dataVal,x1)
			}
		}}

		data := row{dataVal}
		cR.dataRow<-data
	}
	cR.isOver<-true
}


//BY LANHAI:this function will be called to create a csvSR
func GetCSVSelectResult(path string,info []*model.ColumnInfo)(SelectResult, error){
	dataRowChan := make(chan row,1)
	isOver := make(chan bool,1)
	cReader := csvReader{path:path,dataRow:dataRowChan,isOver:isOver,info:info}
	go (&cReader).readFile2()
	return &csvSelectResult{dataRow:dataRowChan,isOver:isOver,info:info},nil
}



type selectResult struct {
	label string
	resp  kv.Response

	results chan resultWithErr
	closed  chan struct{}

	rowLen     int
	fieldTypes []*types.FieldType
	ctx        sessionctx.Context

	selectResp *tipb.SelectResponse
	respChkIdx int

	feedback     *statistics.QueryFeedback
	partialCount int64 // number of partial results.
	sqlType      string
}

func (r *selectResult) Fetch(ctx context.Context) {
	go r.fetch(ctx)
}

func (r *selectResult) fetch(ctx context.Context) {
	startTime := time.Now()
	defer func() {
		close(r.results)
		duration := time.Since(startTime)
		metrics.DistSQLQueryHistgram.WithLabelValues(r.label, r.sqlType).Observe(duration.Seconds())
	}()
	for {
		resultSubset, err := r.resp.Next(ctx)
		if err != nil {
			r.results <- resultWithErr{err: errors.Trace(err)}
			return
		}
		if resultSubset == nil {
			return
		}

		select {
		case r.results <- resultWithErr{result: resultSubset}:
		case <-r.closed:
			// If selectResult called Close() already, make fetch goroutine exit.
			return
		case <-ctx.Done():
			return
		}
	}
}

// NextRaw returns the next raw partial result.
func (r *selectResult) NextRaw(ctx context.Context) ([]byte, error) {
	re := <-r.results
	r.partialCount++
	r.feedback.Invalidate()
	if re.result == nil || re.err != nil {
		return nil, errors.Trace(re.err)
	}
	return re.result.GetData(), nil
}

// Next reads data to the chunk.
func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	for chk.NumRows() < r.ctx.GetSessionVars().MaxChunkSize {
		if r.selectResp == nil || r.respChkIdx == len(r.selectResp.Chunks) {
			err := r.getSelectResp()
			if err != nil || r.selectResp == nil {
				return errors.Trace(err)
			}
		}
		err := r.readRowsData(chk)
		if err != nil {
			return errors.Trace(err)
		}
		if len(r.selectResp.Chunks[r.respChkIdx].RowsData) == 0 {
			r.respChkIdx++
		}
	}
	return nil
}

func (r *selectResult) getSelectResp() error {
	r.respChkIdx = 0
	for {
		re := <-r.results
		if re.err != nil {
			return errors.Trace(re.err)
		}
		if re.result == nil {
			r.selectResp = nil
			return nil
		}
		r.selectResp = new(tipb.SelectResponse)
		err := r.selectResp.Unmarshal(re.result.GetData())
		if err != nil {
			return errors.Trace(err)
		}
		if err := r.selectResp.Error; err != nil {
			return terror.ClassTiKV.New(terror.ErrCode(err.Code), err.Msg)
		}
		sc := r.ctx.GetSessionVars().StmtCtx
		for _, warning := range r.selectResp.Warnings {
			sc.AppendWarning(terror.ClassTiKV.New(terror.ErrCode(warning.Code), warning.Msg))
		}
		r.feedback.Update(re.result.GetStartKey(), r.selectResp.OutputCounts)
		r.partialCount++
		sc.MergeExecDetails(re.result.GetExecDetails())
		if len(r.selectResp.Chunks) == 0 {
			continue
		}
		return nil
	}
}

func (r *selectResult) readRowsData(chk *chunk.Chunk) (err error) {
	rowsData := r.selectResp.Chunks[r.respChkIdx].RowsData
	maxChunkSize := r.ctx.GetSessionVars().MaxChunkSize
	decoder := codec.NewDecoder(chk, r.ctx.GetSessionVars().Location())
	for chk.NumRows() < maxChunkSize && len(rowsData) > 0 {
		for i := 0; i < r.rowLen; i++ {
			rowsData, err = decoder.DecodeOne(rowsData, i, r.fieldTypes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	r.selectResp.Chunks[r.respChkIdx].RowsData = rowsData
	return nil
}

// Close closes selectResult.
func (r *selectResult) Close() error {
	// Close this channel tell fetch goroutine to exit.
	if r.feedback.Actual() >= 0 {
		metrics.DistSQLScanKeysHistogram.Observe(float64(r.feedback.Actual()))
	}
	metrics.DistSQLPartialCountHistogram.Observe(float64(r.partialCount))
	close(r.closed)
	return r.resp.Close()
}
