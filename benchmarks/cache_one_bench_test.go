package benchmarks

import (
	"testing"
	"time"

	"github.com/kamalshkeir/korm"
)

// Test l'avantage du cache pour One() dans différents scénarios
func TestCacheOneAdvantage(t *testing.T) {
	type User struct {
		Id   uint   `korm:"pk"`
		Name string `korm:"size:100"`
	}

	// Setup
	err := korm.AutoMigrate[User]("users_cache_one_test")
	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}
	defer korm.Model[User]().Drop()

	// Insérer 100 users
	for i := 1; i <= 100; i++ {
		_, _ = korm.Model[User]().Insert(&User{Name: "User" + string(rune(i))})
	}

	korm.FlushCache()

	// ==========================================
	// SCENARIO 1 : Lookup répété (N+1 problem)
	// ==========================================
	t.Run("Repeated_Lookups", func(t *testing.T) {
		korm.FlushCache()

		// Première fois : query DB
		start := time.Now()
		user1, _ := korm.Table("users_cache_one_test").Where("id = ?", 1).One()
		first := time.Since(start)

		// Deuxième fois : cache hit
		start = time.Now()
		user2, _ := korm.Table("users_cache_one_test").Where("id = ?", 1).One()
		cached := time.Since(start)

		if user1["id"] != user2["id"] {
			t.Error("Cache returned different data")
		}

		speedup := float64(first) / float64(cached)
		t.Logf("✓ First lookup: %v", first)
		t.Logf("✓ Cached lookup: %v", cached)
		t.Logf("✓ Speedup: %.2fx", speedup)

		if speedup > 10 {
			t.Logf("✅ CACHE VERY BENEFICIAL for repeated One() - %.2fx faster", speedup)
		} else if speedup > 2 {
			t.Logf("✅ CACHE BENEFICIAL for repeated One() - %.2fx faster", speedup)
		} else {
			t.Logf("⚠️  CACHE LOW BENEFIT for repeated One() - only %.2fx faster", speedup)
		}
	})

	// ==========================================
	// SCENARIO 2 : N+1 Problem (boucle)
	// ==========================================
	t.Run("N+1_Problem", func(t *testing.T) {
		korm.FlushCache()

		// Simuler un N+1 : Lookup du même user dans une boucle
		start := time.Now()
		for i := 0; i < 10; i++ {
			_, _ = korm.Table("users_cache_one_test").Where("id = ?", 5).One()
		}
		withCache := time.Since(start)

		// Refaire SANS cache
		korm.FlushCache()
		start = time.Now()
		for i := 0; i < 10; i++ {
			_, _ = korm.Table("users_cache_one_test").Where("id = ?", 5).NoCache().One()
		}
		withoutCache := time.Since(start)

		speedup := float64(withoutCache) / float64(withCache)
		t.Logf("✓ 10 lookups WITH cache: %v", withCache)
		t.Logf("✓ 10 lookups WITHOUT cache: %v", withoutCache)
		t.Logf("✓ Cache saved: %.2fx time", speedup)

		if speedup > 5 {
			t.Logf("✅ CACHE VERY VALUABLE for N+1 scenarios - %.2fx faster", speedup)
		} else {
			t.Logf("⚠️  CACHE BENEFIT for N+1 scenarios - %.2fx faster", speedup)
		}
	})

	// ==========================================
	// SCENARIO 3 : Unique queries (pas de réutilisation)
	// ==========================================
	t.Run("Unique_Queries_No_Reuse", func(t *testing.T) {
		korm.FlushCache()

		// Query différents IDs (jamais relu)
		start := time.Now()
		for i := 1; i <= 10; i++ {
			_, _ = korm.Table("users_cache_one_test").Where("id = ?", i).One()
		}
		withCache := time.Since(start)

		// Sans cache
		korm.FlushCache()
		start = time.Now()
		for i := 1; i <= 10; i++ {
			_, _ = korm.Table("users_cache_one_test").Where("id = ?", i).NoCache().One()
		}
		withoutCache := time.Since(start)

		overhead := float64(withCache) / float64(withoutCache)
		t.Logf("✓ 10 unique queries WITH cache: %v", withCache)
		t.Logf("✓ 10 unique queries WITHOUT cache: %v", withoutCache)
		t.Logf("✓ Overhead: %.2fx", overhead)

		if overhead > 1.1 {
			t.Logf("⚠️  CACHE ADDS OVERHEAD for unique queries - %.2fx slower", overhead)
		} else {
			t.Logf("✅ CACHE NO SIGNIFICANT OVERHEAD for unique queries")
		}
	})
}
