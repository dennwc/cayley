package command

import (
	"net"
	"net/http"
	"time"
	"fmt"
	
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
	KeyHostUI   = "host_ui"
	KeyHostDocs = "host_docs"
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

			chttp.SetupRoutes(h, &config.Config{
				Timeout:  timeout,
				ReadOnly: ro,
				HostUI: viper.GetBool(KeyHostUI),
				HostDocs: viper.GetBool(KeyHostDocs),
			})
			host, _ := cmd.Flags().GetString("host")
			phost := host
			if host, port, err := net.SplitHostPort(host); err == nil && host == "" {
				phost = net.JoinHostPort("localhost", port)
			}

			clog.Infof("listening on %s, web interface at http://%s", host, phost)
			return http.ListenAndServe(host, nil)
		},
	}
	cmd.Flags().String("host", ":64210", "host:port to listen on")
	cmd.Flags().Bool("init", false, "initialize the database before using it")
	cmd.Flags().DurationP("timeout", "t", 30*time.Second, "elapsed time until an individual query times out")
	cmd.Flags().StringVar(&chttp.AssetsPath, "assets", "", "explicit path to the HTTP assets")
	registerLoadFlags(cmd)
	return cmd
}
