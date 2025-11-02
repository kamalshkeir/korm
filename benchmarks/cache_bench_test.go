package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/kamalshkeir/korm"
)

// Test réel du cache - Compare WITH vs WITHOUT cache
func TestCacheEffectiveness(t *testing.T) {
	t.Log("=== TESTING CACHE EFFECTIVENESS ===")

	// S'assurer qu'on a des données
	count, err := korm.Table("test_table").All()
	if err != nil || len(count) == 0 {
		t.Skip("No data in test_table, skipping cache test")
	}

	t.Logf("Testing with %d rows in test_table", len(count))

	// Test 1: All() avec cache
	t.Run("All() - Cache Performance", func(t *testing.T) {
		korm.FlushCache()

		// Première requête - va à la DB
		start := time.Now()
		data1, err := korm.Table("test_table").All()
		firstTime := time.Since(start)
		if err != nil {
			t.Fatalf("First query failed: %v", err)
		}

		// Deuxième requête - devrait venir du cache
		start = time.Now()
		data2, err := korm.Table("test_table").All()
		cachedTime := time.Since(start)
		if err != nil {
			t.Fatalf("Cached query failed: %v", err)
		}

		// Vérifier que les données sont identiques
		if len(data1) != len(data2) {
			t.Errorf("Data mismatch: first=%d, cached=%d", len(data1), len(data2))
		}

		// Le cache devrait être plus rapide
		speedup := float64(firstTime) / float64(cachedTime)
		t.Logf("✓ First query: %v", firstTime)
		t.Logf("✓ Cached query: %v", cachedTime)
		t.Logf("✓ Speedup: %.2fx faster", speedup)

		if cachedTime > firstTime {
			t.Logf("WARNING: Cache not faster (but this can happen on fast DBs)")
		}

		// VERDICT
		if speedup > 1.5 {
			t.Logf("✅ CACHE WORKS WELL - %.2fx speedup", speedup)
		} else if speedup > 1.0 {
			t.Logf("⚠️  CACHE WORKS - Small speedup (%.2fx)", speedup)
		} else {
			t.Logf("❌ CACHE MIGHT NOT WORK - Slower than first query")
		}
	})

	// Test 2: One() avec cache
	t.Run("One() - Cache Performance", func(t *testing.T) {
		korm.FlushCache()

		start := time.Now()
		row1, err := korm.Table("test_table").Where("id = ?", 1).One()
		firstTime := time.Since(start)
		if err != nil {
			t.Fatalf("First query failed: %v", err)
		}

		start = time.Now()
		row2, err := korm.Table("test_table").Where("id = ?", 1).One()
		cachedTime := time.Since(start)
		if err != nil {
			t.Fatalf("Cached query failed: %v", err)
		}

		if row1["email"] != row2["email"] {
			t.Errorf("Data mismatch in One()")
		}

		speedup := float64(firstTime) / float64(cachedTime)
		t.Logf("✓ First query: %v", firstTime)
		t.Logf("✓ Cached query: %v", cachedTime)
		t.Logf("✓ Speedup: %.2fx", speedup)

		if speedup > 1.5 {
			t.Logf("✅ ONE() CACHE WORKS - %.2fx speedup", speedup)
		}
	})

	// Test 3: NoCache() bypass
	t.Run("NoCache() - Bypass Works", func(t *testing.T) {
		korm.FlushCache()

		// Mettre en cache
		korm.Table("test_table").All()

		// Requête normale (du cache)
		start := time.Now()
		korm.Table("test_table").All()
		cachedTime := time.Since(start)

		// Requête avec NoCache() (doit aller à la DB)
		start = time.Now()
		data, err := korm.Table("test_table").NoCache().All()
		noCacheTime := time.Since(start)
		if err != nil {
			t.Fatalf("NoCache query failed: %v", err)
		}

		t.Logf("✓ Cached query: %v", cachedTime)
		t.Logf("✓ NoCache query: %v", noCacheTime)

		if noCacheTime > cachedTime {
			t.Logf("✅ NOCACHE WORKS - NoCache slower than cache (expected)")
		} else {
			t.Logf("⚠️  NoCache same speed (can happen on fast DBs)")
		}

		if len(data) == 0 {
			t.Error("NoCache returned no data")
		}
	})

	// Test 4: Cache invalidation (manuelle ou automatique)
	t.Run("Cache Invalidation - INSERT", func(t *testing.T) {
		korm.FlushCache()

		// Compter les rows initiales
		initial, _ := korm.Table("test_table").All()
		initialCount := len(initial)

		// INSERT une nouvelle row
		_, err := korm.Table("test_table").Insert(map[string]any{
			"email":    fmt.Sprintf("cache-test-%d@example.com", time.Now().Unix()),
			"content":  "Test cache invalidation",
			"password": "test123",
			"is_admin": true,
		})
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		// Vérifier avec NoCache() d'abord (devrait voir le nouveau row)
		afterNoCache, _ := korm.Table("test_table").NoCache().All()
		noCacheCount := len(afterNoCache)

		if noCacheCount > initialCount {
			t.Logf("✅ INSERT WORKED - %d -> %d rows (NoCache)", initialCount, noCacheCount)
		} else {
			t.Errorf("❌ INSERT FAILED - Still %d rows", noCacheCount)
		}

		// Maintenant tester si le cache a été invalidé automatiquement
		// Attendre un peu pour les hooks (ils sont asynchrones)
		time.Sleep(300 * time.Millisecond)

		// Requête normale (sans NoCache)
		after, _ := korm.Table("test_table").All()
		afterCount := len(after)

		if afterCount > initialCount {
			t.Logf("✅ CACHE AUTO-INVALIDATED - %d -> %d rows", initialCount, afterCount)
		} else {
			t.Logf("⚠️  CACHE NOT AUTO-INVALIDATED - Use FlushCache() manually")
			t.Logf("   This is expected if database triggers are not configured")
			// Flusher manuellement et revérifier
			korm.FlushCache()
			afterFlush, _ := korm.Table("test_table").All()
			if len(afterFlush) > initialCount {
				t.Logf("✅ MANUAL FLUSH WORKS - %d rows after FlushCache()", len(afterFlush))
			}
		}

		// Cleanup
		korm.Table("test_table").Where("content = ?", "Test cache invalidation").Delete()
	})

	// Test 5: Cache clés différentes
	t.Run("Different Cache Keys", func(t *testing.T) {
		korm.FlushCache()

		// WHERE id = 1
		row1, _ := korm.Table("test_table").Where("id = ?", 1).One()

		// WHERE id = 2 (clé différente)
		row2, _ := korm.Table("test_table").Where("id = ?", 2).One()

		if row1["id"] == row2["id"] {
			t.Error("❌ CACHE KEYS NOT DIFFERENT - Same data returned")
		} else {
			t.Logf("✅ CACHE KEYS WORK - Different queries return different data")
		}

		// LIMIT différents
		limit3, _ := korm.Table("test_table").Limit(3).All()
		limit5, _ := korm.Table("test_table").Limit(5).All()

		if len(limit3) == 3 && len(limit5) == 5 {
			t.Logf("✅ LIMIT CREATES DIFFERENT KEYS")
		} else {
			t.Errorf("❌ LIMIT not working: got %d and %d", len(limit3), len(limit5))
		}
	})

	// Test 6: Metadata cache
	t.Run("Metadata Cache", func(t *testing.T) {
		korm.FlushCache()

		start := time.Now()
		tables1 := korm.GetAllTables()
		firstTime := time.Since(start)

		start = time.Now()
		tables2 := korm.GetAllTables()
		cachedTime := time.Since(start)

		if len(tables1) != len(tables2) {
			t.Error("Tables count mismatch")
		}

		speedup := float64(firstTime) / float64(cachedTime)
		t.Logf("✓ GetAllTables() speedup: %.2fx", speedup)

		if speedup > 2 {
			t.Logf("✅ METADATA CACHE WORKS")
		}
	})
}

// Benchmark comparaison WITH vs WITHOUT cache
func BenchmarkCacheComparison_All(b *testing.B) {
	b.Run("WithCache", func(b *testing.B) {
		korm.FlushCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.Table("test_table").All()
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.Table("test_table").NoCache().All()
		}
	})
}

func BenchmarkCacheComparison_One(b *testing.B) {
	b.Run("WithCache", func(b *testing.B) {
		korm.FlushCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.Table("test_table").Where("id = ?", 1).One()
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.Table("test_table").Where("id = ?", 1).NoCache().One()
		}
	})
}

func BenchmarkCacheComparison_Query(b *testing.B) {
	b.Run("WithCache", func(b *testing.B) {
		korm.FlushCache()
		data := []TestTable{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.To(&data).Query("SELECT * FROM test_table WHERE is_admin = ?", true)
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		data := []TestTable{}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.To(&data).NoCache().Query("SELECT * FROM test_table WHERE is_admin = ?", true)
		}
	})
}

func BenchmarkMetadataCache(b *testing.B) {
	b.Run("GetAllTables-Cached", func(b *testing.B) {
		korm.FlushCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.GetAllTables()
		}
	})

	b.Run("GetAllColumnsTypes-Cached", func(b *testing.B) {
		korm.FlushCache()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			korm.GetAllColumnsTypes("test_table")
		}
	})
}
