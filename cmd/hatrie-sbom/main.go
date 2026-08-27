package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
)

type module struct {
	Path    string
	Version string
	Main    bool
	Replace *module
}

type document struct {
	SPDXVersion       string     `json:"spdxVersion"`
	DataLicense       string     `json:"dataLicense"`
	SPDXID            string     `json:"SPDXID"`
	Name              string     `json:"name"`
	DocumentNamespace string     `json:"documentNamespace"`
	Packages          []package_ `json:"packages"`
}

type package_ struct {
	SPDXID           string `json:"SPDXID"`
	Name             string `json:"name"`
	VersionInfo      string `json:"versionInfo,omitempty"`
	DownloadLocation string `json:"downloadLocation"`
}

func main() {
	output := flag.String("output", "sbom.spdx.json", "output path")
	flag.Parse()
	command := exec.Command("go", "list", "-m", "-json", "all")
	command.Stderr = os.Stderr
	stream, err := command.Output()
	if err != nil {
		fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(stream))
	modules := []module{}
	for decoder.More() {
		var item module
		if err := decoder.Decode(&item); err != nil {
			fatal(err)
		}
		if item.Replace != nil {
			item.Version = item.Replace.Version
		}
		modules = append(modules, item)
	}
	sort.Slice(modules, func(left, right int) bool { return modules[left].Path < modules[right].Path })
	packages := make([]package_, 0, len(modules))
	for index, item := range modules {
		version := item.Version
		if item.Main && version == "" {
			version = "local"
		}
		packages = append(packages, package_{SPDXID: fmt.Sprintf("SPDXRef-Package-%d", index+1), Name: item.Path, VersionInfo: version, DownloadLocation: "NOASSERTION"})
	}
	doc := document{SPDXVersion: "SPDX-2.3", DataLicense: "CC0-1.0", SPDXID: "SPDXRef-DOCUMENT", Name: "hatrie_cache", DocumentNamespace: "https://github.com/kokizzu/hatrie_cache/sbom", Packages: packages}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fatal(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
