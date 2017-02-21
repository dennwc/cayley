package command

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/cayleygraph/cayley/clog"
	"github.com/cayleygraph/cayley/internal"
	"github.com/cayleygraph/cayley/internal/config"
	chttp "github.com/cayleygraph/cayley/internal/http"
	"github.com/cayleygraph/cayley/quad"
)

const (
	// http
	KeyListen     = "http.listen"
	KeyServeUI    = "http.serve_ui"
	KeyServeDocs  = "http.serve_docs"
	KeyAssetsPath = "http.assets_path"

	DefaultHost = "127.0.0.1"
	DefaultPort = "64210"
)

func NewHttpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP endpoint on the given host and port.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()

			serveDocs := viper.GetBool(KeyServeDocs)
			serveUI := viper.GetBool(KeyServeUI)

			// override assetpath
			chttp.AssetsPath = strings.TrimSpace(viper.GetString(KeyAssetsPath))
			if serveDocs || serveUI {
				clog.Infof("serving ui: %v, serving docs: %v, using assets dir: %s", serveUI, serveDocs, chttp.AssetsPath)
			} else {
				clog.Infof("not serving docs and UI")
			}

			timeout, err := cmd.Flags().GetDuration("timeout")
			if err != nil {
				return err
			}
			if init, err := cmd.Flags().GetBool("init"); err != nil {
				return err
			} else if init {
				if err = initDatabase(); err != nil {
					return err
				}
			}

			h, err := openDatabase()
			if err != nil {
				return err
			}
			defer h.Close()

			ro := viper.GetBool(KeyReadOnly)
			if load, _ := cmd.Flags().GetString(flagLoad); load != "" {
				// don't allow load when config is readonly
				if ro {
					return fmt.Errorf("Can not load into database, read only configuration or flag specified")
				}

				typ, _ := cmd.Flags().GetString(flagLoadFormat)
				if err = internal.Load(h.QuadWriter, quad.DefaultBatch, load, typ); err != nil {
					return err
				}
			}

			// get listen on configuration, should be overridden by command line arguments
			listen := strings.TrimSpace(viper.GetString(KeyListen))

			// do we have a port, otherwise add default port, tested with IPv6
			var findPort = regexp.MustCompile(`:[0-9]+$`)
			if !findPort.MatchString(listen) {
				// net.JoinHostPort does not work without splitting it first
				listen = fmt.Sprintf("%s:%s", listen, DefaultPort)
			}

			host, port, err := net.SplitHostPort(listen)
			if err != nil {
				return err
			}

			if host == "" {
				host = DefaultHost
			}

			listen = net.JoinHostPort(host, port)

			chttp.SetupRoutes(h, &config.Config{
				Timeout:   timeout,
				ReadOnly:  ro,
				ServeUI:   serveUI,
				ServeDocs: serveDocs,
			})

			clog.Infof("listening on %s, web interface at http://%s", host, listen)
			return http.ListenAndServe(listen, nil)
		},
	}
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	registerLoadFlags(cmd)
	return cmd
}
