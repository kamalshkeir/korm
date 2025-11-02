package benchmarks

// Test simple : 2 databases avec la même table, cache ne doit PAS se mélanger
// func TestCacheCrossDatabase(t *testing.T) {
// 	// Setup DB1
// 	err := korm.New(korm.SQLITE, "db1_test", sqlitedriver.Use())
// 	if err != nil {
// 		t.Fatalf("Failed to create db1: %v", err)
// 	}
// 	defer os.Remove("db1_test.sqlite3")

// 	// Setup DB2
// 	err = korm.New(korm.SQLITE, "db2_test", sqlitedriver.Use())
// 	if err != nil {
// 		t.Fatalf("Failed to create db2: %v", err)
// 	}
// 	defer os.Remove("db2_test.sqlite3")

// 	// Créer la même table dans les 2 databases
// 	type User struct {
// 		Id    uint   `korm:"pk"`
// 		Name  string `korm:"size:100"`
// 		Email string `korm:"size:100"`
// 	}

// 	err = korm.AutoMigrate[User]("users", "db1_test")
// 	if err != nil {
// 		t.Fatalf("Failed to migrate db1: %v", err)
// 	}

// 	err = korm.AutoMigrate[User]("users", "db2_test")
// 	if err != nil {
// 		t.Fatalf("Failed to migrate db2: %v", err)
// 	}

// 	// Insérer des données DIFFÉRENTES dans chaque DB
// 	_, err = korm.Table("users").Database("db1_test").Insert(map[string]any{
// 		"name":  "Alice",
// 		"email": "alice@db1.com",
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to insert in db1: %v", err)
// 	}

// 	_, err = korm.Table("users").Database("db2_test").Insert(map[string]any{
// 		"name":  "Bob",
// 		"email": "bob@db2.com",
// 	})
// 	if err != nil {
// 		t.Fatalf("Failed to insert in db2: %v", err)
// 	}

// 	// FLUSH pour commencer propre
// 	korm.FlushCache()

// 	// TEST : Lire DB1 (met en cache)
// 	user1, err := korm.Table("users").Database("db1_test").One()
// 	if err != nil {
// 		t.Fatalf("Failed to read from db1: %v", err)
// 	}

// 	if user1["name"] != "Alice" {
// 		t.Errorf("DB1 should return Alice, got %v", user1["name"])
// 	}

// 	// TEST : Lire DB2 (devrait retourner Bob, PAS Alice du cache)
// 	user2, err := korm.Table("users").Database("db2_test").One()
// 	if err != nil {
// 		t.Fatalf("Failed to read from db2: %v", err)
// 	}

// 	if user2["name"] != "Bob" {
// 		t.Errorf("❌ CROSS-DATABASE CACHE POLLUTION! DB2 returned %v instead of Bob", user2["name"])
// 		t.Errorf("   This means DB2 got DB1's cached data!")
// 	} else {
// 		t.Logf("✅ CROSS-DATABASE CACHE ISOLATION WORKS!")
// 		t.Logf("   DB1 returned: %v", user1["name"])
// 		t.Logf("   DB2 returned: %v", user2["name"])
// 	}

// 	// TEST : Vérifier que les caches sont bien séparés (2ème lecture)
// 	user1Again, _ := korm.Table("users").Database("db1_test").One()
// 	user2Again, _ := korm.Table("users").Database("db2_test").One()

// 	if user1Again["name"] != "Alice" || user2Again["name"] != "Bob" {
// 		t.Errorf("❌ Cache pollution on second read!")
// 		t.Errorf("   DB1 (2nd): %v, DB2 (2nd): %v", user1Again["name"], user2Again["name"])
// 	} else {
// 		t.Logf("✅ Cache remains isolated on multiple reads")
// 	}

// 	// Cleanup
// 	korm.Shutdown("db1_test")
// 	korm.Shutdown("db2_test")
// }
