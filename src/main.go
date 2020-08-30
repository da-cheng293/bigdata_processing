package main

import (
	"encoding/json"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/Shopify/sarama"
	"github.com/vmihailenco/msgpack"
	"github.com/yanyiwu/gojieba"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"time"
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
type fn func (Modify_data, string) Modify_data
func newschina_deal(single_data_res Modify_data, news_china_url string) Modify_data{
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(news_china_url)))

	HandleError(err, "goquery")


	doc.Find("#chan_newsDetail").Find("p").Each(func(i int, selection *goquery.Selection) {

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

	doc.Find("span.source").Each(func(i int, selection *goquery.Selection) {
		fmt.Println(selection.Text())
		single_data_res.Source=single_data_res.Source+selection.Text()

	})


	//single_data_res.ID=sub_id
	return single_data_res
}
func sina_deal(single_data_res Modify_data, sina_url string) Modify_data{
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sina_url)))

		HandleError(err, "goquery")


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


		//single_data_res.ID=sub_id
		return single_data_res
}
func zhibo_deal(single_data_res Modify_data, zhibo_url string) Modify_data{
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(zhibo_url)))

	HandleError(err, "goquery")


	doc.Find(".article-content").Find("p").Each(func(i int, selection *goquery.Selection) {

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

	doc.Find("span.anchor").Each(func(i int, selection *goquery.Selection) {
		fmt.Println(selection.Text())
		single_data_res.Source=single_data_res.Source+selection.Text()

	})


	//single_data_res.ID=sub_id
	return single_data_res
}
func bar(msg string) {
	log.Printf("bar! Message is %s", msg)
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


	parseUrl, err := url.Parse(sub_url)
	HandleError(err, "parseUrl")

	fmt.Println(parseUrl.Host)
	HandleError(err, "parseUrl.Host")

	single_data_res.ID=sub_id
	single_data_res.Title=sub_title
	single_data_res.Timestamp=sub_time
	url_map := map[string] fn {
		"k.sina.com.cn": sina_deal,
		"news.sina.com.cn": sina_deal,
		"news.china.com": newschina_deal,
		"v.zhibo.tv": zhibo_deal,
	}

	value, ok := url_map[parseUrl.Host]
	if ok {
		*data = append(*data, value(single_data_res, sub_url))
	} else {
		fmt.Println("key not found")
	}
	//log.Printf("map is %v", m)
	//m["f"]("Hello")
	//m["b"]("World")







	//
	//if parseUrl.Host=="zz.focus.cn" {
	//	doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))
	//
	//	HandleError(err, "goquery")
	//
	//	fmt.Println(sub_time)
	//	fmt.Println(sub_title)
	//	single_data_res.Title=sub_title
	//	doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {
	//		single_data_res.Body=single_data_res.Body+selection.Text()
	//	})
	//	fmt.Println(single_data_res.Body)
	//	x := gojieba.NewJieba()
	//	defer x.Free()
	//	keywords := x.ExtractWithWeight(single_data_res.Body, 5)
	//	fmt.Println("Extract:", keywords)
	//	for _, elem := range keywords{
	//		single_data_res.Types=append(single_data_res.Types, elem.Word)
	//	}
	//	doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
	//		fmt.Println(selection.Text())
	//		single_data_res.Source=single_data_res.Source+selection.Text()
	//	})
	//	single_data_res.Timestamp=sub_time
	//	single_data_res.ID=sub_id
	//	*data = append(*data, single_data_res )
	//
	//
	//} else if parseUrl.Host == "k.sina.com.cn" || parseUrl.Host =="news.sina.com.cn" {
	//	doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))
	//
	//	HandleError(err, "goquery")
	//	fmt.Println(sub_time)
	//	fmt.Println(sub_title)
	//	single_data_res.Title=sub_title
	//	doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {
	//
	//		single_data_res.Body=single_data_res.Body+selection.Text()
	//	})
	//	fmt.Println(single_data_res.Body)
	//	x := gojieba.NewJieba()
	//	defer x.Free()
	//
	//	keywords := x.ExtractWithWeight(single_data_res.Body, 5)
	//	fmt.Println("Extract:", keywords)
	//
	//	for _, elem := range keywords{
	//		single_data_res.Types=append(single_data_res.Types, elem.Word)
	//	}
	//
	//	doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
	//		fmt.Println(selection.Text())
	//		single_data_res.Source=single_data_res.Source+selection.Text()
	//
	//	})
	//
	//	single_data_res.Timestamp=sub_time
	//	single_data_res.ID=sub_id
	//	*data = append(*data, single_data_res )
	//
	//
	//} else {
	//	doc, err := goquery.NewDocumentFromReader(strings.NewReader(GetPageStr(sub_url)))
	//
	//	HandleError(err, "goquery")
	//	fmt.Println(sub_time)
	//	fmt.Println(sub_title)
	//	single_data_res.Title=sub_title
	//	doc.Find(".article").Find("p").Each(func(i int, selection *goquery.Selection) {
	//
	//		single_data_res.Body=single_data_res.Body+selection.Text()
	//	})
	//	fmt.Println(single_data_res.Body)
	//	x := gojieba.NewJieba()
	//	defer x.Free()
	//
	//	keywords := x.ExtractWithWeight(single_data_res.Body, 5)
	//	fmt.Println("Extract:", keywords)
	//
	//	for _, elem := range keywords{
	//		single_data_res.Types=append(single_data_res.Types, elem.Word)
	//	}
	//
	//	doc.Find("span.author").Each(func(i int, selection *goquery.Selection) {
	//		fmt.Println(selection.Text())
	//		single_data_res.Source=single_data_res.Source+selection.Text()
	//
	//	})
	//
	//	single_data_res.Timestamp=sub_time
	//	single_data_res.ID=sub_id
	//
	//	*data = append(*data, single_data_res )
	//}
}


func main() {

	var responseObject soa
	var data_res []Modify_data   //十个新闻数据 的arrary
	//var data_res_back Modify_data  //接受 从es 读取的数据
	var id_sub int



	urlApi := "http://api.tianapi.com/generalnews/index?key=e522570c5b2737fb6be17f0184bd87d1&page=1&&num=10"
	req, _ := http.NewRequest("GET", urlApi, nil)
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


	broker := "localhost:9092"
	topic := "message_pack"

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	producer, err := sarama.NewAsyncProducer([]string{broker}, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enqueued, errors int
	doneCh := make(chan struct{})
	var data_id int
	data_id=1
	go func() {
		for {

			time.Sleep(1 * time.Second)

			//subject := Subject{
			//	ID:     2,
			//	Title:  "千与千寻",
			//	Genres: []string{"剧情", "喜剧", "爱情", "战争"},
			//}

			b, err := msgpack.Marshal(&data_res)
			if err != nil {
				panic(err)
			}

			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(strconv.Itoa(data_id)),
				Value: sarama.StringEncoder(b),
			}
			select {
			case producer.Input() <- msg:
				enqueued++
				fmt.Printf("Produce message: %v\n", data_res)
			case err := <-producer.Errors():
				errors++
				fmt.Println("Failed to produce message:", err)
			case <-signals:
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)


	//ctx := context.Background()
	//client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL("http://127.0.0.1:9200"))
	//HandleError(err, "newclient")
	//
	//// 用IndexExists检查索引是否存在
	//exists, err := client.IndexExists(indexName).Do(ctx)
	//HandleError(err, "indexexist")
	//fmt.Println("Phone No. = ")
	//if !exists {
	//	// 用CreateIndex创建索引，mapping内容用BodyString传入
	//	_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
	//	HandleError(err, "createindex")
	//}
	//fmt.Println("Phone No. =bbb ")
	//
	//
	//// 写入
	//docEs, err := client.Index().
	//	Index(indexName).
	//	Id(strconv.Itoa(data_res[0].ID)).
	//	BodyJson(data_res[0]).
	//	Refresh("wait_for").
	//	Do(ctx)
	//
	//HandleError(err, "clientindex")
	//fmt.Printf("Indexed with id=%v, type=%s\n", docEs.Id, docEs.Type)
	////读取
	//result, err := client.Get().
	//	Index(indexName).
	//	Id(strconv.Itoa(data_res[0].ID)).
	//	Do(ctx)
	//HandleError(err, "clientget")
	//if result.Found {
	//	fmt.Printf("Got document %v (version=%d, index=%s, type=%s)\n",
	//		result.Id, result.Version, result.Index, result.Type)
	//	err := json.Unmarshal(result.Source, &data_res_back)
	//	HandleError(err, "clientfound")
	//	fmt.Println(data_res_back.ID, data_res_back.Title, data_res_back.Source, data_res_back.Types, data_res_back.Timestamp, data_res_back.Body)
	//}
    ////大量写入
	//bulkRequest := client.Bulk()
	//for _, subject := range data_res {
	//	doc := elastic.NewBulkIndexRequest().Index(indexName).Id(strconv.Itoa(subject.ID)).Doc(subject)
	//	bulkRequest = bulkRequest.Add(doc)
	//}
	//
	//response, err := bulkRequest.Do(ctx)
	//HandleError(err, "bulkrequest")
	//failed := response.Failed()
	//l := len(failed)
	//if l > 0 {
	//	fmt.Printf("Error(%d)", l, response.Errors)
	//}

	}



