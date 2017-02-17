package command

import (
	"fmt"
	"net"
	"net/http"
	"regexp"
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
	KeyListen   = "http.listen"
	KeyHostUI   = "http.path_ui"
	KeyHostDocs = "http.path_docs"

	DefaultHost = "127.0.0.1"
	DefaultPort = "64210"
)

func NewHttpCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "http",
		Short: "Serve an HTTP endpoint on the given host and port.",
		RunE: func(cmd *cobra.Command, args []string) error {
			printBackendInfo()
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

			// first get listen host/port from configuration
			listen := viper.GetString(KeyListen)

			// and see if we have listen as a command-line argument
			cmd_host, _ := cmd.Flags().GetString("listen")

			// command overrides configuration file
			if cmd_host != "" {
				listen = cmd_host
			}

			// do we have a port, otherwise add default port
			var findPort = regexp.MustCompile(`:[0-9]+$`)
			if !findPort.MatchString(listen) {
				listen = net.JoinHostPort(listen, DefaultPort)
			}

			// make sure we both have the host and port to listen on and use default while left empty
			split_host, split_port, _ := net.SplitHostPort(listen)
			if split_host == "" {
				split_host = DefaultHost
			}

			listen = net.JoinHostPort(split_host, split_port)

			chttp.SetupRoutes(h, &config.Config{
				Timeout:  timeout,
				ReadOnly: ro,
				HostUI:   viper.GetString(KeyHostUI),
				HostDocs: viper.GetString(KeyHostDocs),
			})

			clog.Infof("listening on %s, web interface at http://%s", split_host, listen)
			return http.ListenAndServe(listen, nil)
		},
	}
	cmd.Flags().String("listen", "", "host:port to listen on")
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	cmd.Flags().StringVar(&chttp.AssetsPath, "assets", "", "explicit path to the HTTP assets")
	registerLoadFlags(cmd)
	return cmd
}
