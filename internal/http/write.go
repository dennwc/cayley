// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/barakmich/glog"
	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/internal"
	"github.com/google/cayley/quad"
	"github.com/google/cayley/quad/cquads"
	"github.com/google/cayley/quad/json"
)

func quadReaderFromRequest(r *http.Request) (qr quad.ReadCloser) {
	format := quad.FormatByMime(r.Header.Get(`Content-Type`))
	if format != nil && format.Reader != nil {
		qr = format.Reader(r.Body)
	} else {
		qr = json.NewReader(r.Body)
	}
	return
}

func (api *API) ServeV1Write(w http.ResponseWriter, r *http.Request, _ httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}
	qr := quadReaderFromRequest(r)
	defer qr.Close()

	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	n, err := quad.Copy(h, qr)
	if err != nil {
		return jsonResponse(w, 400, err)
	}

	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d quads.\"}", n)
	return 200
}

func (api *API) ServeV1WriteNQuad(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}

	formFile, _, err := r.FormFile("NQuadFile")
	if err != nil {
		glog.Errorln(err)
		return jsonResponse(w, 500, "Couldn't read file: "+err.Error())
	}
	defer formFile.Close()

	blockSize, blockErr := strconv.ParseInt(r.URL.Query().Get("block_size"), 10, 64)
	if blockErr != nil {
		blockSize = int64(api.config.LoadSize)
	}

	quadReader, err := internal.Decompressor(formFile)
	// TODO(kortschak) Make this configurable from the web UI.
	dec := cquads.NewDecoder(quadReader)

	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}

	n, err := quad.CopyBatch(h, dec, int(blockSize))
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully wrote %d quads.\"}", n)

	return 200
}

func (api *API) ServeV1Delete(w http.ResponseWriter, r *http.Request, params httprouter.Params) int {
	if api.config.ReadOnly {
		return jsonResponse(w, 400, "Database is read-only.")
	}
	qr := quadReaderFromRequest(r)
	quads, err := quad.ReadAll(qr)
	qr.Close()
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	h, err := api.GetHandleForRequest(r)
	if err != nil {
		return jsonResponse(w, 400, err)
	}
	count := 0
	for _, q := range quads {
		h.QuadWriter.RemoveQuad(q)
		count++
	}
	fmt.Fprintf(w, "{\"result\": \"Successfully deleted %d quads.\"}", count)
	return 200
}
