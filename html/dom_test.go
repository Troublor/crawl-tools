package html

import "testing"

func TestFetchDom(t *testing.T) {
	url := "https://etherscan.io/blocks_forked?ps=100&p=2"
	dom, err := FetchDom(url)
	if err != nil {
		t.Fatal(err)
	}
	if dom == nil {
		t.Fatal("fetched dom is nil")
	}
}
