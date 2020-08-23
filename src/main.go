package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/olivere/elastic"
	"github.com/yanyiwu/gojieba"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"

	"strconv"
	"strings"
)


const mapping = `
{
	"mappings": {
		
		"properties": {
			"Timestamp": {
				"type": "text"
			},
            "Source": {
				"type": "text"
			},
			"Title": {
				"type": "text"
			},
            "Body": {
				"type": "text"
			},
			"Types": {
				"type": "text"
			}
		}
	}
}`

var (

	indexName = "data_res"
	typeName = "online"
	servers   = "http://localhost:9200/"
)


type soa struct {
	Newslist    []Newslist `json:"Newslist"`
}


type Newslist struct {

	Ctime               string `json:"ctime"`
	Title               string `json:"title"`
	Description         string `json:"description"`
	PicUrl              string `json:"picUrl"`
	Url                 string `json:"url"`

}

type Modify_data struct {
	ID                      int            `json:"id"`
	Timestamp               string `json:"Timestamp"`
	Source					string `json:"Source"`
	Title               string `json:"title"`
	Body        string `json:"body"`
	Types              []string `json:"Types"`

}

func HandleError(err error, why string) {
	if err != nil {
		fmt.Println(why, err)
	}
}

func GetPageStr(url string) (pageStr string) {
	//1.发送http请求，获取页面内容
	resp, err := http.Get(url)
	//处理异常
	HandleError(err, "http.Get url")
	//关闭资源
	defer resp.Body.Close()
	//接收页面
	pageBytes, err := ioutil.ReadAll(resp.Body)
	HandleError(err, "ioutil.ReadAll")
	//打印页面内容
	pageStr = string(pageBytes)
	//fmt.Println("OK", pageStr)
	return pageStr
}


func Get_datares(data *[]Modify_data, sub_title string, sub_time string, sub_url string, sub_id int)  {

	var single_data_res Modify_data


	u, err := url.Parse(sub_url)
	if err != nil {
		panic(err)
	}

	fmt.Println(u.Host)
	if err != nil {

		log.Fatal(err)

	}

	if u.Host=="zz.focus.cn" {
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))

		if err != nil {

			log.Fatal(err)

		}

		fmt.Println(sub_time)
		fmt.Println(sub_title)
		single_data_res.Title=sub_title
		doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {
			single_data_res.Body=single_data_res.Body+selection.Text()
		})
		fmt.Println(single_data_res.Body)
		x := gojieba.NewJieba()
		defer x.Free()
		keywords := x.ExtractWithWeight(single_data_res.Body, 5)
		fmt.Println("Extract:", keywords)
		for _, elem := range keywords{
			single_data_res.Types=append(single_data_res.Types, elem.Word)
		}
		doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
			fmt.Println(selection.Text())
			single_data_res.Source=single_data_res.Source+selection.Text()
		})
		single_data_res.Timestamp=sub_time
		single_data_res.ID=sub_id
		*data = append(*data, single_data_res )


	} else if u.Host == "k.sina.com.cn" || u.Host =="news.sina.com.cn" {
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))

		if err != nil {

			log.Fatal(err)

		}
		fmt.Println(sub_time)
		fmt.Println(sub_title)
		single_data_res.Title=sub_title
		doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {

			single_data_res.Body=single_data_res.Body+selection.Text()
		})
		fmt.Println(single_data_res.Body)
		x := gojieba.NewJieba()
		defer x.Free()

		keywords := x.ExtractWithWeight(single_data_res.Body, 5)
		fmt.Println("Extract:", keywords)

		for _, elem := range keywords{
			single_data_res.Types=append(single_data_res.Types, elem.Word)
		}

		doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
			fmt.Println(selection.Text())
			single_data_res.Source=single_data_res.Source+selection.Text()

		})

		single_data_res.Timestamp=sub_time
		single_data_res.ID=sub_id
		*data = append(*data, single_data_res )


	} else {
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))

		if err != nil {

			log.Fatal(err)

		}
		fmt.Println(sub_time)
		fmt.Println(sub_title)
		single_data_res.Title=sub_title
		doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {

			single_data_res.Body=single_data_res.Body+selection.Text()
		})
		fmt.Println(single_data_res.Body)
		x := gojieba.NewJieba()
		defer x.Free()

		keywords := x.ExtractWithWeight(single_data_res.Body, 5)
		fmt.Println("Extract:", keywords)

		for _, elem := range keywords{
			single_data_res.Types=append(single_data_res.Types, elem.Word)
		}

		doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
			fmt.Println(selection.Text())
			single_data_res.Source=single_data_res.Source+selection.Text()

		})

		single_data_res.Timestamp=sub_time
		single_data_res.ID=sub_id

		*data = append(*data, single_data_res )
	}
}


func main() {

	var responseObject soa
	var data_res []Modify_data   //十个新闻数据 的arrary
	var data_res_back Modify_data  //接受 从es 读取的数据
	var id_sub int



	urll := "http://api.tianapi.com/generalnews/index?key=e522570c5b2737fb6be17f0184bd87d1&page=1&&num=10"
	req, _ := http.NewRequest("GET", urll, nil)
	res, _ := http.DefaultClient.Do(req)
	defer res.Body.Close()
	fmt.Println("var1 = ", reflect.TypeOf(res.Body))
	body, _ := ioutil.ReadAll(res.Body)
	fmt.Println("Phone No. = ", string(body))

	json.Unmarshal(body, &responseObject)


	fmt.Println(len(responseObject.Newslist))

	for i := 0; i < len(responseObject.Newslist); i++ {
		fmt.Println(responseObject.Newslist[i].Url)
		title_sub := responseObject.Newslist[i].Title
		Ctime_sub := responseObject.Newslist[i].Ctime
		url_sub := responseObject.Newslist[i].Url
		id_sub=id_sub+1
		Get_datares(&data_res, title_sub, Ctime_sub, url_sub, id_sub)
	}
	for j :=0; j<len(data_res);j++{
		fmt.Println(data_res[j])
	}
	ctx := context.Background()
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://127.0.0.1:9200"))
	if err != nil {
		panic(err)
	}

	// 用IndexExists检查索引是否存在
	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("Phone No. = ")
	if !exists {
		// 用CreateIndex创建索引，mapping内容用BodyString传入
		_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Phone No. =bbb ")


	// 写入
	docc, err := client.Index().
		Index(indexName).
		Id(strconv.Itoa(data_res[0].ID)).
		BodyJson(data_res[0]).
		Refresh("wait_for").
		Do(ctx)

	if err != nil {
		panic(err)
	}
	fmt.Printf("Indexed with id=%v, type=%s\n", docc.Id, docc.Type)
	//读取
	result, err := client.Get().
		Index(indexName).
		Id(strconv.Itoa(data_res[0].ID)).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	if result.Found {
		fmt.Printf("Got document %v (version=%d, index=%s, type=%s)\n",
			result.Id, result.Version, result.Index, result.Type)
		err := json.Unmarshal(result.Source, &data_res_back)
		if err != nil {
			panic(err)
		}
		fmt.Println(data_res_back.ID, data_res_back.Title, data_res_back.Source, data_res_back.Types, data_res_back.Timestamp, data_res_back.Body)
	}
    //大量写入
	bulkRequest := client.Bulk()
	for _, subject := range data_res {
		doc := elastic.NewBulkIndexRequest().Index(indexName).Id(strconv.Itoa(subject.ID)).Doc(subject)
		bulkRequest = bulkRequest.Add(doc)
	}

	response, err := bulkRequest.Do(ctx)
	if err != nil {
		panic(err)
	}
	failed := response.Failed()
	l := len(failed)
	if l > 0 {
		fmt.Printf("Error(%d)", l, response.Errors)
	}

	}



