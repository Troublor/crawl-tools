package html

import (
	"github.com/PuerkitoBio/goquery"
	"net/http"
)

/**
Given a url, return a goquery.Document pointer to the dom tree of the website
*/
func FetchDom(url string) (*goquery.Document, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, err
	}
	dom, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}
	return dom, nil
}
