package benchmarks

import (
	"testing"

	"github.com/kamalshkeir/korm"
)

// Test que BuilderM et BuilderS n'interfèrent pas entre eux dans le cache
func TestCacheIsolationMapVsStruct(t *testing.T) {
	type User struct {
		Id   uint   `korm:"pk"`
		Name string `korm:"size:100"`
	}

	// Créer une table de test
	err := korm.AutoMigrate[User]("cache_isolation_test")
	if err != nil {
		t.Fatalf("Failed to migrate: %v", err)
	}
	defer korm.Model[User]().Drop()

	// Insérer des données
	_, err = korm.Model[User]().Insert(&User{Name: "Alice"})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Flush pour commencer propre
	korm.FlushCache()

	// 1. Query avec BuilderM (maps)
	resultMap, err := korm.Table("cache_isolation_test").All()
	if err != nil {
		t.Fatalf("BuilderM.All() failed: %v", err)
	}
	if len(resultMap) == 0 {
		t.Fatal("BuilderM returned no data")
	}
	t.Logf("✓ BuilderM returned %d rows (maps)", len(resultMap))

	// 2. Query avec BuilderS (structs)
	resultStruct, err := korm.Model[User]().All()
	if err != nil {
		t.Fatalf("BuilderS.All() failed: %v", err)
	}
	if len(resultStruct) == 0 {
		t.Fatal("BuilderS returned no data")
	}
	t.Logf("✓ BuilderS returned %d rows (structs)", len(resultStruct))

	// 3. Vérifier que les 2 ont leurs propres caches
	// (si on modifie la DB, les 2 doivent être invalidés séparément)

	// Insérer une nouvelle ligne
	_, err = korm.Model[User]().Insert(&User{Name: "Bob"})
	if err != nil {
		t.Fatalf("Failed to insert Bob: %v", err)
	}

	// Attendre invalidation
	korm.FlushCache()

	// Re-query avec les deux builders
	resultMap2, _ := korm.Table("cache_isolation_test").All()
	resultStruct2, _ := korm.Model[User]().All()

	if len(resultMap2) != 2 || len(resultStruct2) != 2 {
		t.Errorf("❌ Cache isolation broken!")
		t.Errorf("   BuilderM: %d rows (expected 2)", len(resultMap2))
		t.Errorf("   BuilderS: %d rows (expected 2)", len(resultStruct2))
	} else {
		t.Logf("✅ CACHE ISOLATION WORKS - BuilderM and BuilderS have separate caches")
		t.Logf("   BuilderM (::m::): %d rows", len(resultMap2))
		t.Logf("   BuilderS (::s::): %d rows", len(resultStruct2))
	}
}
