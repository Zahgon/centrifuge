package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
)

type clientMessage struct {
	Timestamp int64  `json:"timestamp"`
	Input     string `json:"input"`
}

func handleLog(e centrifuge.LogEntry) { _ = "STUB: not implemented"; return }

type connectData struct {
	Email string `json:"email"`
}

type contextKey int

var ginContextKey contextKey

// GinContextToContextMiddleware - at the resolver level we only have access
// to context.Context inside centrifuge, but we need the gin context. So we
// create a gin middleware to add its context to the context.Context used by
// centrifuge websocket server.
func GinContextToContextMiddleware() gin.HandlerFunc {
	_ = "STUB: not implemented"
	return *new(gin.HandlerFunc)
}

// GinContextFromContext - we recover the gin context from the context.Context
// struct where we added it just above
func GinContextFromContext(ctx context.Context) (*gin.Context, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Finally we can use gin context in the auth middleware of centrifuge.
func authMiddleware(h http.Handler) http.Handler {
	_ = "STUB: not implemented"
	return *new(http.Handler)
}

// We get gin ctx from context.Context struct.

// And now we can access gin session.

func main() {
	node, _ := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelDebug,
		LogHandler: handleLog,
	})

	node.OnConnecting(func(ctx context.Context, event centrifuge.ConnectEvent) (centrifuge.ConnectReply, error) {
		// Let's include user email into connect reply, so we can display username in chat.
		// This is an optional step, actually.
		cred, ok := centrifuge.GetCredentials(ctx)
		if !ok {
			return centrifuge.ConnectReply{}, centrifuge.DisconnectServerError
		}
		data, _ := json.Marshal(connectData{
			Email: cred.UserID,
		})
		return centrifuge.ConnectReply{
			Data: data,
		}, nil
	})

	node.OnConnect(func(client *centrifuge.Client) {
		transport := client.Transport()
		log.Printf("user %s connected via %s.", client.UserID(), transport.Name())

		// Connect handler should not block, so start separate goroutine to
		// periodically send messages to client.
		go func() {
			for {
				select {
				case <-client.Context().Done():
					return
				case <-time.After(5 * time.Second):
					err := client.Send([]byte(`{"time": "` + strconv.FormatInt(time.Now().Unix(), 10) + `"}`))
					if err != nil {
						if err == io.EOF {
							return
						}
						log.Println(err.Error())
					}
				}
			}
		}()

		client.OnRefresh(func(e centrifuge.RefreshEvent, cb centrifuge.RefreshCallback) {
			log.Printf("user %s connection is going to expire, refreshing", client.UserID())
			cb(centrifuge.RefreshReply{
				ExpireAt: time.Now().Unix() + 10,
			}, nil)
		})

		client.OnSubscribe(func(e centrifuge.SubscribeEvent, cb centrifuge.SubscribeCallback) {
			log.Printf("user %s subscribes on %s", client.UserID(), e.Channel)
			cb(centrifuge.SubscribeReply{
				Options: centrifuge.SubscribeOptions{
					EmitPresence: true,
				},
			}, nil)
		})

		client.OnPresence(func(e centrifuge.PresenceEvent, cb centrifuge.PresenceCallback) {
			cb(centrifuge.PresenceReply{}, nil)
		})

		client.OnUnsubscribe(func(e centrifuge.UnsubscribeEvent) {
			log.Printf("user %s unsubscribed from %s", client.UserID(), e.Channel)
		})

		client.OnPublish(func(e centrifuge.PublishEvent, cb centrifuge.PublishCallback) {
			log.Printf("user %s publishes into channel %s: %s", client.UserID(), e.Channel, string(e.Data))
			var msg clientMessage
			err := json.Unmarshal(e.Data, &msg)
			if err != nil {
				cb(centrifuge.PublishReply{}, centrifuge.ErrorBadRequest)
				return
			}
			cb(centrifuge.PublishReply{}, nil)
		})

		client.OnRPC(func(e centrifuge.RPCEvent, cb centrifuge.RPCCallback) {
			log.Printf("[user %s] sent RPC, data: %s, method: %s", client.UserID(), string(e.Data), e.Method)
			switch e.Method {
			case "getCurrentYear":
				cb(centrifuge.RPCReply{Data: []byte(`{"year": "2020"}`)}, nil)
			default:
				cb(centrifuge.RPCReply{}, centrifuge.ErrorMethodNotFound)
			}
		})

		client.OnDisconnect(func(e centrifuge.DisconnectEvent) {
			log.Printf("user %s disconnected, disconnect: %s", client.UserID(), e.Disconnect)
		})
	})

	// We also start a separate goroutine for centrifuge itself, since we
	// still need to run gin web server.
	go func() {
		if err := node.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	r := gin.Default()
	store := cookie.NewStore([]byte("secret_string"))
	r.Use(sessions.Sessions("session_name", store))
	r.LoadHTMLFiles("./login_form.html", "./chat.html")
	// Here we tell gin to use the middleware we created just above
	r.Use(GinContextToContextMiddleware())

	r.GET("/login", func(c *gin.Context) {
		s := sessions.Default(c)
		if s.Get("user") != nil && s.Get("user").(string) == "email@email.com" {
			c.Redirect(http.StatusMovedPermanently, "/chat")
			c.Abort()
		} else {
			c.HTML(200, "login_form.html", gin.H{})
		}
	})

	r.POST("/login", func(c *gin.Context) {
		email := c.PostForm("email")
		passwd := c.PostForm("password")
		s := sessions.Default(c)
		if email == "email@email.com" && passwd == "password" {
			s.Set("user", email)
			_ = s.Save()
			c.Redirect(http.StatusMovedPermanently, "/chat")
			c.Abort()
		} else {
			c.JSON(403, gin.H{
				"message": "Bad email/password combination",
			})
		}
	})

	r.GET("/connection/websocket", gin.WrapH(authMiddleware(centrifuge.NewWebsocketHandler(node, centrifuge.WebsocketConfig{}))))

	r.GET("/chat", func(c *gin.Context) {
		s := sessions.Default(c)
		if s.Get("user") != nil {
			c.HTML(200, "chat.html", gin.H{})
		} else {
			c.JSON(403, gin.H{
				"message": "Not logged in!",
			})
		}
		c.Abort()
	})

	_ = r.Run() // listen and serve on 0.0.0.0:8080
}
