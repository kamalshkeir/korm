package korm

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kamalshkeir/kmap"
	"github.com/kamalshkeir/ksbus"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/ksmux/ws"
	"github.com/kamalshkeir/lg"
)

var (
	nodeManager      *NodeManager
	nodeManagerDebug = false
)

// Node represents a KORM node in the cluster
type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Active  bool   `json:"active"`
	Secure  bool   `json:"secure"`
}

// NodeManager handles node registration and data synchronization
type NodeManager struct {
	nodes        *kmap.SafeMap[string, *Node]
	server       *ksbus.Server
	database     string
	secure       bool
	inSync       bool
	stopChan     chan struct{}
	stopChanOnce sync.Once
}

func DebugNodeManager() {
	nodeManagerDebug = true
}

func (nm *NodeManager) startHeartbeat() {
	nm.stopChan = make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-nm.stopChan:
				return
			case <-ticker.C:
				nodes := nm.GetNodes()
				for _, node := range nodes {
					// Try to ping the node
					err := nm.server.PublishToServer(node.Address, map[string]any{
						"type": "ping",
						"addr": nm.server.Address,
						"id":   nm.server.ID,
					}, node.Secure)

					wasActive := node.Active
					if err != nil {
						if n, ok := nm.nodes.Get(node.Address); ok {
							n.Active = false
						}
					} else {
						if n, ok := nm.nodes.Get(node.Address); ok {
							n.Active = true
							// If node was previously inactive and is now active
							if !wasActive {
								// Trigger a full sync
								go func(n *Node) {
									if err := nm.SyncData(n); err != nil {
										lg.ErrorC("Failed to sync data on reconnection:", "err", err)
									}
								}(n)
							}
						}
					}
				}
			}
		}
	}()
}

func GetDefaultDbMem() *DatabaseEntity {
	db, err := GetMemoryDatabase(defaultDB)
	if err != nil {
		if len(databases) == 0 {
			return nil
		} else {
			return &databases[0]
		}
	}
	return db
}

func WithNodeManager() *NodeManager {
	if nodeManager != nil {
		return nodeManager
	}
	if serverBus == nil {
		serverBus = WithBus()
	}
	serverBus.OnServerData(onServerData)
	// Create node manager after server is initialized
	nodeManager = newNodeManager(serverBus)
	initNodeManagerHooks(nodeManager)
	if strings.HasPrefix(nodeManager.server.Address, ":") {
		nodeManager.server.Address = "localhost" + nodeManager.server.Address
	}
	if nodeManagerDebug {
		fmt.Println("Server ID:", nodeManager.server.ID)
	}
	db := GetDefaultDbMem()
	if db.Name != "" {
		dbTe := GetTablesInfosFromDB()
		for _, teDB := range dbTe {
			found := false
			for _, t := range db.Tables {
				if t.Name == teDB.Name {
					found = true
				}
			}
			if !found {
				db.Tables = append(db.Tables, teDB)
			}
		}
	}
	initHandlersDashboard(nodeManager.server.App)
	return nodeManager
}

func initHandlersDashboard(app *ksmux.Router) {
	app.Get("/admin/nodemanager", Admin(func(c *ksmux.Context) {
		nodes := nodeManager.GetNodes()
		secureNodes := 0
		activeNodes := 0
		for _, n := range nodes {
			if n.Active {
				activeNodes++
			}
			if n.Secure {
				secureNodes++
			}
		}
		c.Html("admin/admin_nodemanager.html", map[string]any{
			"nodes":       nodes,
			"activeNodes": activeNodes,
			"secureNodes": secureNodes,
		})
	}))
	app.Post("/admin/nodemanager/restart", Admin(func(c *ksmux.Context) {
		var data struct {
			Address string `json:"address"`
		}
		if err := c.BindBody(&data); err != nil {
			c.Status(400).Json(map[string]any{"error": "invalid json"})
			return
		}

		// If it's our address, restart self
		if data.Address == nodeManager.server.Address ||
			"localhost"+data.Address == nodeManager.server.Address {
			go func() {
				time.Sleep(100 * time.Millisecond) // Small delay to allow response to be sent
				nodeManager.GracefulRestart()
			}()
			c.Status(200).Json(map[string]any{"message": "restarting self"})
			return
		}

		// Otherwise send restart message to remote node
		err := nodeManager.server.PublishToServer(data.Address, map[string]any{
			"type": "restart_node",
			"addr": nodeManager.server.Address,
			"id":   nodeManager.server.ID,
		}, nodeManager.IsSecure(data.Address))

		if err != nil {
			c.Status(500).Json(map[string]any{"error": "failed tco send restart command"})
			return
		}

		c.Status(200).Json(map[string]any{"message": "restart initiated"})
	}))
	app.Get("/admin/nodemanager/nodes/list", Admin(func(c *ksmux.Context) {
		nodes := nodeManager.GetNodes()
		secureNodes := 0
		activeNodes := 0
		for _, n := range nodes {
			if n.Active {
				activeNodes++
			}
			if n.Secure {
				secureNodes++
			}
		}
		c.Json(map[string]any{
			"nodes":  nodes,
			"total":  len(nodes),
			"active": activeNodes,
			"secure": secureNodes,
		})
	}))
	app.Post("/admin/nodemanager/nodes/add", Admin(func(c *ksmux.Context) {
		var data struct {
			Address string `json:"address"`
			Secure  bool   `json:"secure"`
		}
		if err := c.BindBody(&data); err != nil {
			c.Error("invalid json")
			return
		}
		if data.Address == "" {
			c.Error("invalid json")
			return
		}
		n := nodeManager.GetNode(data.Address)
		if n != nil {
			c.Error("node already exists")
			return
		}
		targetNode := &Node{
			Address: data.Address,
			Secure:  data.Secure,
		}
		nodeManager.AddNode(targetNode)
		nodeManager.SyncData(targetNode)
		nodes := nodeManager.GetNodes()
		secureNodes := 0
		activeNodes := 0
		for _, n := range nodes {
			if n.Active {
				activeNodes++
			}
			if n.Secure {
				secureNodes++
			}
		}
		c.Json(map[string]any{
			"success": "Node added",
			"nodes":   nodes,
			"total":   len(nodes),
			"secure":  secureNodes,
			"active":  activeNodes,
		})
	}))
	app.Post("/admin/nodemanager/nodes/remove", Admin(func(c *ksmux.Context) {
		var data struct {
			Address string `json:"address"`
		}
		if err := c.BindBody(&data); err != nil {
			c.Error("invalid json")
			return
		}
		if data.Address == "" {
			c.Error("invalid json")
			return
		}
		n := nodeManager.GetNode(data.Address)
		if n == nil {
			fmt.Println("node not found")
			c.Error("node not found")
			return
		}
		// send to the removed server to remove me too
		_ = nodeManager.server.PublishToServer(data.Address, map[string]any{
			"type": "node_offline",
			"addr": nodeManager.server.Address,
			"id":   nodeManager.server.ID,
		}, n.Secure)
		nodeManager.RemoveNode(data.Address)
		nodes := nodeManager.GetNodes()
		secureNodes := 0
		activeNodes := 0
		for _, n := range nodes {
			if n.Active {
				activeNodes++
			}
			if n.Secure {
				secureNodes++
			}
		}
		c.Json(map[string]any{
			"success": "Node removed",
			"nodes":   nodes,
			"total":   len(nodes),
			"secure":  secureNodes,
			"active":  activeNodes,
		})
	}))
	ksmux.OnShutdown(func(srv *http.Server) error {
		nodeManager.Shutdown()
		return nil
	})
}

func (nm *NodeManager) GracefulRestart() error {
	// 1. Notify other nodes we're going down for restart
	nodes := nm.GetNodes()
	for _, node := range nodes {
		_ = nm.server.PublishToServer(node.Address, map[string]any{
			"type": "node_restart",
			"addr": nm.server.Address,
			"id":   nm.server.ID,
		}, node.Secure)
	}

	// 2. Wait a bit for messages to be sent
	time.Sleep(100 * time.Millisecond)

	nm.nodes.Flush()

	Shutdown()

	// 5. Properly shutdown the HTTP server and all its connections
	if nm.server != nil && nm.server.App != nil && nm.server.App.Server != nil {
		// Set a short timeout for existing connections to finish
		nm.server.App.Server.SetKeepAlivesEnabled(false)

		// Create a context with timeout for shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Shutdown the server
		if err := nm.server.App.Server.Shutdown(ctx); err != nil {
			lg.ErrorC("Error shutting down server:", "err", err)
		}

		// Close the underlying listener
		if nm.server.App.Server.Handler != nil {
			if closer, ok := nm.server.App.Server.Handler.(io.Closer); ok {
				closer.Close()
			}
		}
	}

	// 6. Set global nodeManager to nil
	nodeManager = nil

	// 7. Wait for resources to be released
	time.Sleep(500 * time.Millisecond)

	// 8. Start the new process before killing the current one
	self, err := os.Executable()
	if err != nil {
		return err
	}

	// Create a new process with the same arguments and environment
	cmd := exec.Command(self, os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = os.Environ()

	// Start the new process
	if err := cmd.Start(); err != nil {
		return err
	}

	// 9. Give the new process time to start
	time.Sleep(1 * time.Second)
	ksmux.Graceful(func() error {
		Shutdown()
		nm.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = nm.server.App.Server.Shutdown(ctx)
		return nil
	})

	// 10. Exit the current process
	os.Exit(0)
	return nil
}

func initNodeManagerHooks(nodeManager *NodeManager) {
	// Add hook for data changes
	OnInsert(func(hd HookData) {
		if nodeManager != nil && !nodeManager.inSync {
			nodes := nodeManager.GetNodes()
			if nodeManagerDebug {
				fmt.Println("----------------------------")
				fmt.Println("OnInsert:", hd)
				fmt.Println("PUB TO NODES", nodes)
				fmt.Println("----------------------------")
			}
			for _, node := range nodes {
				if err := nodeManager.server.PublishToServer(node.Address, map[string]any{
					"type":  "insert_rec",
					"id":    nodeManager.server.ID,
					"addr":  nodeManager.server.Address,
					"table": hd.Table,
					"pk":    hd.Pk,
					"data":  hd.Data,
				}, node.Secure); err != nil {
					lg.ErrorC("Failed to sync insert:", "err", err)
				}
			}
		}
	})

	// Add hook for updates
	OnSet(func(hd HookData) {
		if nodeManager != nil && !nodeManager.inSync {
			nodes := nodeManager.GetNodes()
			if nodeManagerDebug {
				fmt.Println("----------------------------")
				fmt.Println("OnSet:", hd)
				fmt.Println("PUB TO NODES", nodes)
				fmt.Println("----------------------------")
			}
			for _, node := range nodes {
				if err := nodeManager.server.PublishToServer(node.Address, map[string]any{
					"type":     "update_rec",
					"id":       nodeManager.server.ID,
					"addr":     nodeManager.server.Address,
					"table":    hd.Table,
					"pk":       hd.Pk,
					"old_data": hd.Old,
					"new_data": hd.New,
				}, node.Secure); err != nil {
					lg.ErrorC("Failed to sync set:", "err", err)
				}
			}
		}
	})

	// Add hook for deletes
	OnDelete(func(hd HookData) {
		if nodeManager != nil && !nodeManager.inSync {
			nodes := nodeManager.GetNodes()
			if nodeManagerDebug {
				fmt.Println("----------------------------")
				fmt.Println("OnDelete:", hd)
				fmt.Println("PUB TO NODES", nodes)
				fmt.Println("----------------------------")
			}
			for _, node := range nodes {
				if err := nodeManager.server.PublishToServer(node.Address, map[string]any{
					"type":  "delete_rec",
					"id":    nodeManager.server.ID,
					"addr":  nodeManager.server.Address,
					"table": hd.Table,
					"pk":    hd.Pk,
					"data":  hd.Data,
				}, node.Secure); err != nil {
					lg.ErrorC("Failed to sync insert:", "err", err)
				}
			}
		}
	})

	// Add hook for drops
	OnDrop(func(hd HookData) {
		if nodeManager != nil && !nodeManager.inSync {
			nodes := nodeManager.GetNodes()
			if nodeManagerDebug {
				fmt.Println("----------------------------")
				fmt.Println("OnDrop:", hd)
				fmt.Println("PUB TO NODES", nodes)
				fmt.Println("----------------------------")
			}
			for _, node := range nodes {
				if err := nodeManager.server.PublishToServer(node.Address, map[string]any{
					"type":  "drop_table",
					"id":    nodeManager.server.ID,
					"addr":  nodeManager.server.Address,
					"table": hd.Table,
				}, node.Secure); err != nil {
					lg.ErrorC("Failed to sync insert:", "err", err)
				}
			}
		}
	})
}

func onServerData(data any, _ *ws.Conn) {
	if nodeManager == nil {
		return
	}
	var (
		msg map[string]any
		ok  bool
	)
	if msg, ok = data.(map[string]any); !ok {
		return
	}
	nodeManager.inSync = true
	defer func() {
		if nodeManager != nil {
			nodeManager.inSync = false
		}
	}()
	// db, _ := GetMemoryDatabase(defaultDB)
	switch msg["type"] {
	case "node_offline":
		if addr, ok := msg["addr"].(string); ok {
			nodeManager.RemoveNode(addr)
		}
	case "ping":
		// Respond to ping
		// id := msg["id"].(string)
		addr := msg["addr"].(string)
		err := nodeManager.server.PublishToServer(addr, map[string]any{
			"type": "pong",
			"addr": nodeManager.server.Address,
			"id":   nodeManager.server.ID,
		}, nodeManager.IsSecure(addr))
		if err != nil {
			lg.ErrorC("Failed to respond to ping:", "err", err)
		}
	case "initsync":
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("initsync sending all tables in chunks to remote", id, addr)
			fmt.Println("----------------------------")
		}
		_ = nodeManager.SyncData(&Node{
			ID:      id,
			Address: addr,
		})
	case "migrate":
		// receive missing tables, migrate them and send ready to initsync all tables
		// addr := msg["addr"].(string)
		statements := msg["statements"].([]any)
		allTablesMemAny := msg["tablesMem"].([]any)
		addr := msg["addr"].(string)
		db, err := GetMemoryDatabase(defaultDB)
		if lg.CheckError(err) {
			return
		}
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("migrate from", addr)
			for _, s := range statements {
				fmt.Println("stat:", s)
			}
			for _, tb := range allTablesMemAny {
				fmt.Println("table mem:", tb)
			}
		}
		triggers := []string{}
		for _, tmemAny := range allTablesMemAny {
			if tableMem, ok := tmemAny.(map[string]any); ok {
				found := false
				for _, tdb := range db.Tables {
					if tbString, ok := tableMem["Table"].(string); ok {
						if tdb.Name == tbString {
							found = true
						}
					}
				}
				if !found {
					colsAny := tableMem["Columns"].([]any)
					typesAny := tableMem["Types"].(map[string]any)
					modelTypesAny := tableMem["ModelTypes"].(map[string]any)
					tagsAny := tableMem["Tags"].(map[string]any)
					fkeysAny := tableMem["Fkeys"].([]any)
					pk := tableMem["Pk"].(string)
					cols := make([]string, 0, len(colsAny))
					for _, v := range colsAny {
						cols = append(cols, v.(string))
					}
					typess := make(map[string]string, len(typesAny))
					for k, v := range typesAny {
						typess[k] = v.(string)
					}
					modelTypes := make(map[string]string, len(modelTypesAny))
					for k, v := range modelTypesAny {
						modelTypes[k] = v.(string)
					}
					tags := make(map[string][]string, len(tagsAny))
					for k, v := range tagsAny {
						ss := make([]string, 0, len(v.([]any)))
						for _, vv := range v.([]any) {
							ss = append(ss, vv.(string))
						}
						tags[k] = ss
					}
					fkeys := make([]kormFkey, 0, len(fkeysAny))
					for _, v := range fkeysAny {
						if vv, ok := v.(map[string]any); ok {
							fkeys = append(fkeys, kormFkey{
								FromTableField: vv["FromTableField"].(string),
								ToTableField:   vv["ToTableField"].(string),
								Unique:         vv["Unique"].(bool),
							})
						}
					}

					if tbname, ok := tableMem["Name"].(string); ok {
						tbmem := TableEntity{
							Columns:    cols,
							Name:       tbname,
							Pk:         pk,
							Fkeys:      fkeys,
							Types:      typess,
							Tags:       tags,
							ModelTypes: modelTypes,
						}
						if nodeManagerDebug {
							fmt.Printf("adding table mem: %+v\n", tbmem)
						}
						db.Tables = append(db.Tables, tbmem)
						triggers = append(triggers, tbname)
					}

				}
			}
		}

		for _, st := range statements {
			if vmap, ok := st.(map[string]any); ok {
				stat := vmap["Statement"].(string)
				if nodeManagerDebug {
					fmt.Println("executing stat:", stat)
				}
				_, err := GetConnection().Exec(stat)
				if lg.CheckError(err) {
					continue
				}
			}
		}
		m := map[string]bool{}
		for _, tr := range triggers {
			if _, ok := m[tr]; !ok {
				if tr != "_triggers_queue" {
					if nodeManagerDebug {
						fmt.Println("adding changes trigger for", tr)
					}
					err = AddChangesTrigger(tr)
					lg.CheckError(err)
					m[tr] = true
				}
			}
		}
		flushCache()
		if err := nodeManager.server.PublishToServer(addr, map[string]any{
			"type": "initsync",
			"addr": nodeManager.server.Address,
			"id":   nodeManager.server.ID,
		}, nodeManager.IsSecure(addr)); err != nil {
			if nodeManagerDebug {
				fmt.Println("ERROR: failed to sync data to node", "targetNode.Addr", addr, "err", err)
			}
			return
		}
		if nodeManagerDebug {
			fmt.Println("----------------------------")
		}
	case "addNode":
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("new JOIN request from node", msg["id"], msg["addr"])
		}
		// new node request to join
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		secure := msg["secure"].(bool)
		dialect := msg["dialect"].(string)
		tablesInS := msg["tables"].(string)
		tablesIn := strings.Split(tablesInS, ",")
		tables := GetAllTables()
		nf := []string{}

		// Always update the node with the latest ID
		if existingNode, exists := nodeManager.nodes.Get(addr); exists {
			existingNode.ID = id
			existingNode.Active = true
			existingNode.Secure = secure
		} else {
			newNode := &Node{
				Address: addr,
				ID:      id,
				Secure:  secure,
				Active:  true,
			}
			nodeManager.AddNode(newNode)
		}

		// Send back our node info to update the remote node's list
		if err := nodeManager.server.PublishToServer(addr, map[string]any{
			"type":   "node_info",
			"addr":   nodeManager.server.Address,
			"id":     nodeManager.server.ID,
			"secure": nodeManager.secure,
		}, secure); err != nil {
			lg.ErrorC("failed to send node info", "err", err)
		}

		// check not found tables in remote node
		for _, t := range tables {
			found := false
			for _, tIn := range tablesIn {
				if t == tIn {
					found = true
				}
			}
			if !found {
				nf = append(nf, t)
			}
		}
		// if remote missing tables, create migration statement for these tables
		// migration statement handle requested dialect
		type MMigration struct {
			Table     string
			Statement string
			Dialect   string
		}
		dataToSend := []MMigration{}
		db, _ := GetMemoryDatabase(defaultDB)
		for _, tname := range nf {
			// use requested dialect instead of our dialect
			vDB := *db
			vDB.Dialect = dialect
			data := MMigration{
				Table:   tname,
				Dialect: dialect,
			}
			if v, ok := mModelTablename[tname]; ok {
				// create migrate statement
				stat, err := autoMigrateAny(v, &vDB, tname, false)
				if lg.CheckError(err) {
					return
				}
				data.Statement = stat
			}

			dataToSend = append(dataToSend, data)
		}
		if nodeManagerDebug {
			fmt.Println("tables not found on remote:", nf)
		}
		// send to remote migrate statement for missing tables
		if err := nodeManager.server.PublishToServer(addr, map[string]any{
			"type":       "migrate",
			"addr":       nodeManager.server.Address,
			"id":         nodeManager.server.ID,
			"tables":     nf,
			"statements": dataToSend,
			"tablesMem":  GetTablesInfosFromDB(),
		}, nodeManager.IsSecure(addr)); err != nil {
			lg.ErrorC("failed to sync data to node", "targetNode.Addr", addr, "err", err)
			return
		}
		if nodeManagerDebug {
			fmt.Println("----------------------------")
		}
	case "sync_data":
		// receive chunk tables, apply
		table := msg["table"].(string)
		page := msg["page"].(float64)
		count := msg["count"].(float64)
		pk := ""
		if pkField, ok := msg["table_pk"].(string); ok {
			pk = pkField
		}
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("got chunks for table:", table)
			fmt.Println("page:", page)
			fmt.Println("count:", count)
		}

		// Convert incoming records to proper format
		recordsAny := msg["records"].([]any)
		records := make([]map[string]any, 0, len(recordsAny))
		for _, r := range recordsAny {
			if v, ok := r.(map[string]any); ok {
				records = append(records, v)
			}
		}

		// Process each incoming record
		for _, rec := range records {
			if len(rec) == 0 {
				continue
			}

			pkVal := rec[pk]
			if pkVal == nil {
				continue
			}

			// Check if record exists
			existing, err := Table(table).Where(pk+"=?", pkVal).One()
			if err != nil {
				// Record doesn't exist, insert it
				if nodeManagerDebug {
					fmt.Printf("inserting new record in %s: %v\n", table, rec)
				}
				_, err := Table(table).Insert(rec)
				if nodeManagerDebug && err != nil {
					fmt.Printf("Insert failed for %s: %v\n", table, err)
				}
			} else {
				// Record exists, check if data is different
				needsUpdate := false
				updateData := make(map[string]any)

				for k, v := range rec {
					if k == pk {
						continue
					}
					if !reflect.DeepEqual(existing[k], v) {
						needsUpdate = true
						updateData[k] = v
					}
				}

				if needsUpdate {
					if nodeManagerDebug {
						fmt.Printf("updating %s with pk = %v with data %v\n", table, pkVal, updateData)
					}
					_, err := Table(table).Where(pk+"=?", pkVal).SetM(updateData)
					if nodeManagerDebug && err != nil {
						fmt.Printf("Update failed for %s: %v\n", table, err)
					}
				} else if nodeManagerDebug {
					fmt.Printf("skipping update for %s with pk = %v (no changes)\n", table, pkVal)
				}
			}
		}
		if nodeManagerDebug {
			fmt.Println("----------------------------")
		}
	case "insert_rec":
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		table := msg["table"].(string)
		pk := msg["pk"].(string)
		data := msg["data"].(map[string]any)
		pkID := int(data[pk].(float64))

		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("Inserting Record", table, data)
			fmt.Println("id:", id)
			fmt.Println("addr:", addr)
			fmt.Println("table:", table)
			fmt.Println("pk:", pk)
			fmt.Println("data:", data)
			fmt.Println("----------------------------")
		}

		// Check if record exists
		_, err := Table(table).Where(pk+"=?", pkID).One()
		if err != nil {
			// Don't delete the pk from data, keep it for insert
			_, err = Table(table).Insert(data)
		} else {
			// For updates, we can remove the pk
			delete(data, pk)
			_, err = Table(table).Where(pk+"=?", pkID).SetM(data)
		}
		if err != nil {
			lg.ErrorC("unable to create or update", "table", table, "pk", pkID, "err", err)
			return
		}
		if dahsboardUsed {
			data[pk] = pkID
			nodeManager.server.Publish("korm_db_dashboard_hooks", msg)
		}
	case "update_rec":
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		table := msg["table"].(string)
		pk := msg["pk"].(string)
		oldData := msg["old_data"].(map[string]any)
		newData := msg["new_data"].(map[string]any)
		pkID := int(oldData[pk].(float64))
		delete(oldData, pk)
		delete(newData, pk)
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("Updating Record", table, oldData[pk])
			fmt.Println("id:", id)
			fmt.Println("addr:", addr)
			fmt.Println("table:", table)
			fmt.Println("pkId:", pkID)
			fmt.Println("oldData:", oldData)
			fmt.Println("newData:", newData)
			fmt.Println("----------------------------")
		}

		// Compare old and new data before updating
		if !mapsEqual(oldData, newData) {
			_, err := Table(table).Where(pk+"=?", pkID).SetM(newData)
			if err != nil {
				lg.ErrorC("unable to update", "table", table, "pk", pkID, "err", err)
				return
			}
			if dahsboardUsed {
				oldData[pk] = pkID
				newData[pk] = pkID
				nodeManager.server.Publish("korm_db_dashboard_hooks", msg)
			}
		}
	case "delete_rec":
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		table := msg["table"].(string)
		pk := msg["pk"].(string)
		data := msg["data"].(map[string]any)
		pkID := int(data[pk].(float64))
		delete(data, pk)
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("Deleting Record", table, data[pk])
			fmt.Println("id:", id)
			fmt.Println("addr:", addr)
			fmt.Println("table:", table)
			fmt.Println("pk:", pk)
			fmt.Println("data:", data)
			fmt.Println("----------------------------")
		}

		_, err := Table(table).Where(pk+"=?", pkID).Delete()
		if err != nil {
			lg.ErrorC("unable to update", "table", table, "pk", pkID, "err", err)
			return
		}
		if dahsboardUsed {
			data[pk] = pkID
			nodeManager.server.Publish("korm_db_dashboard_hooks", msg)
		}
	case "drop_table":
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		table := msg["table"].(string)
		if nodeManagerDebug {
			fmt.Println("----------------------------")
			fmt.Println("Dropping Table", table)
			fmt.Println("id:", id)
			fmt.Println("addr:", addr)
			fmt.Println("table:", table)
			fmt.Println("----------------------------")
		}
		_, err := Table(table).Drop()
		if err != nil {
			lg.ErrorC("unable to update", "table", table, "err", err)
			return
		}
		if dahsboardUsed {
			nodeManager.server.Publish("korm_db_dashboard_hooks", msg)
		}
	case "node_restart":
		if addr, ok := msg["addr"].(string); ok {
			// Temporarily mark node as inactive
			if n, ok := nodeManager.nodes.Get(addr); ok {
				n.Active = false
			}
		}
	case "restart_node":
		// Received restart command from another node
		go func() {
			time.Sleep(100 * time.Millisecond)
			nodeManager.GracefulRestart()
		}()
	case "node_info":
		// Update node info received from remote node
		id := msg["id"].(string)
		addr := msg["addr"].(string)
		secure := msg["secure"].(bool)

		if existingNode, exists := nodeManager.nodes.Get(addr); exists {
			existingNode.ID = id
			existingNode.Active = true
			existingNode.Secure = secure
		}
	}
}

func RestartSelf() error {
	self, err := os.Executable()
	if err != nil {
		return err
	}
	args := os.Args
	env := os.Environ()
	pid := os.Getpid()

	// Start the new process
	cmd := exec.Command(self, args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = env

	// Start the new process
	if err := cmd.Start(); err != nil {
		return err
	}

	// Give the new process time to start
	time.Sleep(500 * time.Millisecond)

	// Kill the current process
	if runtime.GOOS == "windows" {
		// On Windows, use taskkill with specific PID
		killCmd := exec.Command("taskkill", "/F", "/PID", fmt.Sprint(pid))
		killCmd.Run() // Ignore error as the process will be killed anyway
	} else {
		// On Unix systems, use SIGTERM
		proc, err := os.FindProcess(pid)
		if err == nil {
			proc.Signal(syscall.SIGTERM)
		}
	}

	// This line might not be reached due to the kill
	os.Exit(0)
	return nil
}

// Helper function to compare maps
func mapsEqual(m1, m2 map[string]any) bool {
	if len(m1) != len(m2) {
		return false
	}
	for k, v1 := range m1 {
		if v2, ok := m2[k]; !ok || v1 != v2 {
			return false
		}
	}
	return true
}

// newNodeManager creates a new node manager
func newNodeManager(server *ksbus.Server, secure ...bool) *NodeManager {
	sec := false
	if len(secure) > 0 && secure[0] {
		sec = true
	}
	nodes := kmap.New[string, *Node]()
	nm := &NodeManager{
		nodes:    nodes,
		server:   server,
		database: defaultDB,
		inSync:   false,
		secure:   sec,
	}
	nm.startHeartbeat()
	return nm
}

func (nm *NodeManager) AddNode(node *Node) {
	if nm == nil {
		return
	}
	if node.Address == "" {
		lg.ErrorC("node address empty")
		return
	}
	if strings.HasPrefix(node.Address, ":") {
		node.Address = "localhost" + node.Address
	}
	// Set node as active initially
	node.Active = true
	nm.nodes.Set(node.Address, node)

	tables := GetAllTables(defaultDB)
	db, _ := GetMemoryDatabase(defaultDB)
	// connect
	if err := nm.server.PublishToServer(node.Address, map[string]any{
		"type":    "addNode",
		"addr":    nm.server.Address,
		"id":      nm.server.ID,
		"tables":  strings.Join(tables, ","),
		"dialect": db.Dialect,
		"secure":  nm.secure,
	}, node.Secure); err != nil {
		if nodeManagerDebug {
			lg.ErrorC("failed to sync data to node", "targetNode.Addr", node.Address, "err", err)
		}
		return
	}
}

func (nm *NodeManager) RemoveNode(nodeAddr string) {
	if nm == nil {
		return
	}
	nm.nodes.Delete(nodeAddr)
}

func (nm *NodeManager) GetNodes() []*Node {
	if nm == nil {
		return nil
	}
	nodes := make([]*Node, 0, nm.nodes.Len())

	nm.nodes.Range(func(key string, value *Node) bool {
		nodes = append(nodes, value)
		return true
	})

	return nodes
}

func (nm *NodeManager) GetNode(addr string) *Node {
	if nm == nil {
		return nil
	}
	var n *Node
	nm.nodes.Range(func(key string, value *Node) bool {
		if value.Address == addr {
			n = value
			return false
		}
		return true
	})
	return n
}

func GetNodeManager() *NodeManager {
	return nodeManager
}

// SyncData send all tables to targetNode sync_data
func (nm *NodeManager) SyncData(targetNode *Node) error {
	if nm == nil {
		return fmt.Errorf("not available")
	}
	// Get all tables from the current database
	tables := GetAllTables()
	if len(tables) == 0 {
		return fmt.Errorf("no tables found")
	}

	db, _ := GetMemoryDatabase(defaultDB)

	// For each table, sync data
	for _, table := range tables {
		if table == "_tables_infos" {
			continue // Skip _tables_infos as it's handled by migration
		}

		var t TableEntity
		for _, tt := range db.Tables {
			if tt.Name == table {
				t = tt
			}
		}

		// Get all records from the current table
		page := 1
		for {
			data, err := Table(table).Limit(50).Page(page).All()
			if err != nil {
				if err == ErrNoData {
					break // No more data for this table
				}
				lg.ErrorC("error getting data:", "table", table, "err", err)
				break
			}
			if len(data) == 0 {
				break
			}

			if nodeManagerDebug {
				fmt.Printf("Syncing %d records from table %s (page %d)\n", len(data), table, page)
			}

			// Clean the data before sending
			cleanData := make([]map[string]any, 0, len(data))
			for _, record := range data {
				cleanRecord := make(map[string]any)
				for k, v := range record {
					cleanKey := strings.ReplaceAll(k, "`", "")
					cleanRecord[cleanKey] = v
				}
				cleanData = append(cleanData, cleanRecord)
			}

			// Send sync data message to target node
			syncData := map[string]any{
				"type":     "sync_data",
				"table":    table,
				"records":  cleanData,
				"table_pk": t.Pk,
				"page":     page,
				"count":    len(cleanData),
			}

			if err := nm.server.PublishToServer(targetNode.Address, syncData, nm.IsSecure(targetNode.Address)); err != nil {
				lg.ErrorC("failed to sync data to node", "targetNode.Id", targetNode.ID, "err", err)
				return err
			}

			page++
			time.Sleep(200 * time.Millisecond)
		}
	}

	return nil
}

func (nm *NodeManager) IsSecure(addr string) bool {
	if nm == nil {
		return false
	}
	if v, ok := nm.nodes.Get(addr); !ok {
		return false
	} else {
		return v.Secure
	}
}

func (nm *NodeManager) Shutdown() {
	if nm == nil {
		return
	}
	// Notify other nodes that we're going offline
	nodes := nm.GetNodes()
	for _, node := range nodes {
		// Try to notify each node of our departure
		_ = nm.server.PublishToServer(node.Address, map[string]any{
			"type": "node_offline",
			"addr": nm.server.Address,
			"id":   nm.server.ID,
		}, node.Secure)
	}

	// Clear the nodes map
	nm.nodes.Flush()

	if nm.stopChan != nil {
		nm.stopChanOnce.Do(func() {
			close(nm.stopChan)
		})
	}

	// Set global nodeManager to nil
	nodeManager = nil
}
