# ⚡ KORM : Le Guide Ultime du Fullstack Go Moderne
### Un seul binaire. Tout inclus. Des performances inégalées.

> *"Pourquoi déployer Django + Daphne + Celery + Redis + Nginx + Certbot quand un seul binaire Go de 25 Mo peut gérer la base de données, l'ORM, le routage HTTP, les WebSockets temps réel, le Dashboard Admin, les emails et les certificats TLS à plus de 40 000 requêtes/seconde ?"*

---

## 📑 Sommaire
1. [La Philosophie : The 1-Binary Fullstack Paradigm](#1-la-philosophie--the-1-binary-fullstack-paradigm)
2. [Benchmarks : KORM vs GORM](#2-benchmarks--korm-vs-gorm)
3. [Quickstart : Votre première application complète](#3-quickstart--votre-première-application-complète)
4. [Modélisation & Migrations Automatiques](#4-modélisation--migrations-automatiques)
5. [Query Builder Ultra-Fluide](#5-query-builder-ultra-fluide)
6. [Cache Intelligent & Invalidation Temps Réel](#6-cache-intelligent--invalidation-temps-réel)
7. [SQL Hooks & Triggers Applicatifs](#7-sql-hooks--triggers-applicatifs)
8. [Le Dashboard Admin : Django sous stéroïdes](#8-le-dashboard-admin--django-sous-stéroïdes)
9. [Shell Interactif & CLI Dédié](#9-shell-interactif--cli-dédié)
10. [Réseau Temps Réel : Le Bus Pub/Sub (ksps)](#10-réseau-temps-réel--le-bus-pubsub-ksps)
11. [Serveur & Client Email Complet (ksmtp)](#11-serveur--client-email-complet-ksmtp)
12. [Reverse Proxy & Load Balancer Intégrés](#12-reverse-proxy--load-balancer-intégrés)
13. [Déploiement VPS : 1 Commande, Auto-HTTPS Let's Encrypt](#13-déploiement-vps--1-commande-auto-https-lets-encrypt)

---

## 1. La Philosophie : The 1-Binary Fullstack Paradigm

Le développement web moderne s'est embourbé dans la complexité : microservices superflus, conteneurs Docker empilés, dépendances NPM incontrôlables et reverse proxies fragiles.

**KORM** renverse la table en offrant un écosystème Fullstack Go unifié :

* **Zéro Reverse Proxy requis** : KORM et son routeur HTTP **`ksmux`** intègrent la négociation automatique des certificats SSL/TLS Let's Encrypt avec redirection HTTP -> HTTPS autonome.
* **Zéro Redis requis** : Grâce au bus réseau haute performance **`ksps`** et au système de cache natif synchronisé par goroutines et channels.
* **Zéro Nginx requis pour les assets** : Templates HTML et fichiers statiques (CSS, JS, images) sont compilables directement dans le binaire via `embed.FS`.
* **Zéro CMS/Admin tiers** : Un Admin Dashboard PWA complet, élégant, réactif et temps réel est inclus en **une seule ligne de code**.
* **Déploiement VPS en 10 secondes** : `go build` ➔ `scp` ➔ `systemctl restart app` ➔ **Terminé**.

---

## 2. Benchmarks : KORM vs GORM

KORM a été conçu dès le premier jour pour éliminer l'overhead colossal des allocations mémoire et de la réflexion lente que l'on retrouve dans les ORMs traditionnels comme GORM.

Voici les résultats de tests réels (*Intel Core i5-7300HQ CPU @ 2.50GHz*) :

### 🚀 Récupération de 7 000 lignes
| Opération | Vitesse (ns/op) | Mémoire (B/op) | Allocs (allocs/op) | Facteur d'accélération |
| :--- | :--- | :--- | :--- | :--- |
| **GORM** (`Find`) | **56 049 832 ns** *(56 ms)* | **12 163 316 B** *(12 MB)* | **328 790** | 1x (Base) |
| **KORM** (`Model().All()`) | **395 ns** *(0.0003 ms)* | **224 B** | **1** | **x141 800 plus rapide** |

### 🔍 Récupération d'un enregistrement unique
| Opération | Vitesse (ns/op) | Mémoire (B/op) | Allocs (allocs/op) |
| :--- | :--- | :--- | :--- |
| **GORM** (`First`) | **96 988 ns** | **5 930 B** | **142** |
| **KORM** (`Model().One()`) | **805 ns** | **336 B** | **7** |

### 📄 Pagination (100 éléments)
* **GORM** : `738 934 ns/op` — `165 423 B/op` — `4 704 allocs/op`
* **KORM** : `956 ns/op` — `400 B/op` — `7 allocs/op` (770x plus rapide)

> **Pourquoi un tel écart ?**  
> KORM intègre un cache en mémoire intelligent couplé à une invalidation atomique ultra-légère. Vos lectures récurrentes sont servies à la vitesse de la RAM, tout en restant toujours fraîches et cohérentes grâce aux triggers automatiques sur chaque mutation (`INSERT`, `UPDATE`, `DELETE`, `DROP`).

---

## 3. Quickstart : Votre première application complète

Créez un projet prêt pour la production en un seul fichier `main.go` :

```go
package main

import (
	"embed"
	"strings"

	"github.com/kamalshkeir/aes"
	"github.com/kamalshkeir/korm"
	"github.com/kamalshkeir/ksmux"
	"github.com/kamalshkeir/lg"
	"github.com/kamalshkeir/sqlitedriver"
)

//go:embed assets/static assets/templates
var EmbeddedAssets embed.FS

// 1. Définition des Modèles
type Article struct {
	Id        uint   `korm:"pk"`
	Title     string `korm:"size:255;notnull"`
	Slug      string `korm:"size:255;unique;notnull"`
	Content   string `korm:"text"`
	Published bool   `korm:"default:false"`
	AuthorId  uint   `korm:"fk:users.id:cascade:cascade"`
}

func main() {
	// Clé secrète pour le chiffrement des sessions AES
	aes.SetSecret("votre-cle-secrete-ultra-longue-32-chars")

	// 2. Connexion à la base de données (SQLite, Postgres, MySQL, MariaDB, Cockroach)
	err := korm.New(korm.SQLITE, "production", sqlitedriver.Use())
	if lg.CheckError(err) {
		return
	}
	defer korm.Shutdown()

	// 3. Migration automatique
	err = korm.AutoMigrate[Article]("articles")
	lg.CheckError(err)

	// 4. Dashboard Admin Django-style + Bus Temps Réel
	srv := korm.WithDashboard(":9313", korm.DashOpts{
		Path:               "/admin",
		WithTerminal:       true, // Terminal Web intégré dans le dashboard
		WithKanban:         true, // Tableau Kanban intégré
		WithRequestCounter: true, // Statistiques de requêtes en direct
	})

	// 5. Shell CLI interactif (accessible via terminal)
	korm.WithShell()

	// 6. Routes Métier avec ksmux
	app := srv.App()
	app.Use(ksmux.Gzip())

	app.Get("/", func(c *ksmux.Context) {
		articles, err := korm.Model[Article]().Where("published = ?", true).All()
		if err != nil {
			c.Status(500).Json(map[string]string{"error": err.Error()})
			return
		}
		c.Json(articles)
	})

	// 7. Lancement : Local ou Production Auto-HTTPS
	domain := "monsite.com"
	if strings.Contains(domain, ".") {
		app.Config.Domain = domain
		app.Config.SubDomains = []string{"www." + domain}
		srv.RunAutoTLS() // Let's Encrypt automatique sur port 80 & 443 !
	} else {
		srv.Run() // Serveur HTTP local
	}
}
```

---

## 4. Modélisation & Migrations Automatiques

KORM analyse vos structs Go et génère le DDL SQL parfait pour le dialecte ciblé.

### Tags supportés
* `korm:"pk"` : Clé primaire auto-incrémentée (ou UUID/ULID selon le type).
* `korm:"fk:table.column:on_delete:on_update"` : Clé étrangère avec cascades (ex: `cascade`, `restrict`, `set_null`).
* `korm:"unique"` : Contrainte d'unicité.
* `korm:"index"` : Index standard pour accélérer les requêtes.
* `korm:"notnull"` : Contrainte NOT NULL.
* `korm:"default:valeur"` : Valeur par défaut.
* `korm:"size:255"` : Taille maximale de la colonne.
* `korm:"text"` : Type TEXT / LONGTEXT.
* `korm:"check:age >= 18"` : Contrainte CHECK au niveau base de données.
* `korm:"generated:col1 || ' ' || col2"` : Colonne `GENERATED ALWAYS AS` (compatible tous dialectes).
* `korm:"nocache"` : Exclut ce modèle ou champ du cache mémoire.
* `korm:"-"` : Ignore le champ lors de la migration et des requêtes.

### Relations Avancées
```go
type User struct {
	Id       uint      `korm:"pk"`
	Email    string    `korm:"unique;notnull"`
	Profile  Profile   // 1-to-1 relation
	Articles []Article // 1-to-Many relation
	Roles    []Role    `korm:"m2m:user_roles:user_id:role_id"` // Many-to-Many
}
```

### Schema Diff & Dry Run
Vous avez modifié une struct en Go ? KORM sait comparer l'état de votre code avec la base réelle :
```go
// Affiche le différentiel de schéma sans l'exécuter
diffs, err := korm.Diff()

// Ou exécutez directement via le Shell KORM :
// $ diff -a
```

---

## 5. Query Builder Ultra-Fluide

Deux manières intuitives de manipuler vos données selon vos préférences :

### A. Model Builder Typé (`korm.Model[T]()`)
```go
// 1. Insertion
id, err := korm.Model[Article]().Insert(&Article{
    Title:     "L'ascension de Go Fullstack",
    Slug:      "ascension-go-fullstack",
    Content:   "Développer avec un seul binaire...",
    Published: true,
    AuthorId:  1,
})

// 2. Sélection avec filtres et relations
article, err := korm.Model[Article]().
    Where("slug = ?", "ascension-go-fullstack").
    Limit(1).
    One()

// 3. Pagination express
articles, err := korm.Model[Article]().
    Where("published = ?", true).
    OrderBy("id DESC").
    Page(1, 20).All() // Page 1, 20 éléments par page

// 4. Mise à jour ciblée
affected, err := korm.Model[Article]().
    Where("id = ?", id).
    Update("published", false)

// 5. Suppression
affected, err := korm.Model[Article]().
    Where("id = ?", id).
    Delete()
```

### B. Table Builder Dynamique (`korm.Table("name")`)
Idéal pour le reporting, les jointures complexes ou la manipulation dynamique :
```go
rows, err := korm.Table("articles").
    Where("published = ? AND views > ?", true, 1000).
    OrderBy("-views").
    All()
```

### C. Requêtes Nommées (Named Queries)
Finies les confusions d'index de paramètres `?` ou `$1` :
```go
user, err := korm.Table("users").
    WhereNamed("email = :email AND status = :status", map[string]any{
        "email":  "kamal@example.com",
        "status": "active",
    }).One()
```

---

## 6. Cache Intelligent & Invalidation Temps Réel

Le secret des performances stratosphériques de KORM réside dans son architecture de cache :

1. **Lectures ultra-rapides** : Dès qu'une requête est exécutée, ses résultats sont indexés en RAM.
2. **Invalidation par Goroutine & Channel** : Dès qu'une requête d'écriture (`Insert`, `Update`, `Delete`, `Drop`) survient sur une table, KORM émet un signal non-bloquant sur un canal Go. Une goroutine dédiée se charge de nettoyer immédiatement les entrées concernées.
3. **Résultat** : Zéro stale data (données périmées) et des débits de lecture mesurés en **nanosecondes**.

---

## 7. SQL Hooks & Triggers Applicatifs

Exécutez de la logique métier immédiatement après une modification en base, même si elle a été effectuée hors de l'ORM via un trigger SQL !

```go
// Hook à l'insertion
korm.OnInsert(func(data korm.HookData) {
    lg.Info("Nouvelle ligne insérée !", "table", data.Table, "pk", data.Pk, "data", data.Data)
    // Exemple : Déclencher une notification WebSocket ou envoyer un email
})

// Hook à la modification
korm.OnSet(func(data korm.HookData) {
    lg.Info("Mise à jour détectée", "table", data.Table, "old", data.Old, "new", data.New)
})

// Hook à la suppression
korm.OnDelete(func(data korm.HookData) {
    lg.Warn("Ligne supprimée", "table", data.Table, "pk", data.Pk)
})
```

---

## 8. Le Dashboard Admin : Django sous stéroïdes

Une seule ligne suffit pour doter votre application d'une interface d'administration luxueuse :
```go
srv := korm.WithDashboard(":9313")
```

### Fonctionnalités embarquées :
* 📱 **PWA & Offline Ready** : Service Worker et Manifest intégrés.
* 🎛️ **CRUD Dynamique** : Création, modification, recherche textuelle, tri et suppression de toutes vos tables.
* 📊 **Graphique de Schéma & Relations** : Visualisation interactive des liens de clés étrangères.
* 📡 **Live Logs WebSockets** : Les logs émis via `lg` sont streamés en direct dans le navigateur sans rafraîchissement.
* 💻 **Web Terminal** (`WithTerminal: true`) : Exécutez des commandes shell ou des requêtes SQL directement depuis l'UI.
* 📋 **Tableau Kanban** (`WithKanban: true`) : Gestionnaire de tâches complet inclus pour l'équipe.
* 📈 **Métriques Système & Requêtes** : Visualisez l'état du serveur, de la mémoire et des temps de réponse.
* 🔄 **Restart à Chaud** : Redémarrez le binaire depuis l'interface sécurisée.

---

## 9. Shell Interactif & CLI Dédié

Besoin d'intervenir rapidement sur le serveur ? KORM embarque un Shell REPL puissant :

```go
korm.WithShell()
```

Depuis le terminal, lancez :
```sh
go run main.go shell
# ou ./mon-binaire shell
```

### Commandes disponibles :
* `createsuperuser` : Crée instantanément un compte administrateur sécurisé par **Argon2id** pour le Dashboard.
* `tables` : Liste toutes les tables de la base.
* `columns <table_name>` : Détaille les types et contraintes des colonnes.
* `diff -a` : Inspecte les différences de schéma et applique les migrations SQL.
* `query SELECT * FROM users LIMIT 5` : Exécute une requête SQL brute.
* `getall articles` : Affiche les enregistrements dans une vue tabulaire.
* `exec migration.sql` : Exécute un script SQL complet.

---

## 10. Réseau Temps Réel : Le Bus Pub/Sub (ksps)

Besoin de synchroniser plusieurs instances ou de pousser des données en direct aux clients Web ou à des scripts Python ? Le bus **`ksps`** est là :

```go
// 1. Récupération du bus serveur
bus := korm.WithBus()

// 2. Souscription à un topic
bus.Subscribe("articles:updates", func(msg ksps.Message, unsub func()) {
    lg.Info("Notification reçue", "data", msg.Data)
})

// 3. Publication d'un événement
bus.Publish("articles:updates", map[string]any{
    "action": "new_article",
    "id":     42,
})

// 4. Publication avec accusé de réception (Ack)
ack := bus.PublishWithAck("critical:event", payload, 2*time.Second)
if ack.Ok {
    lg.Info("Message confirmé par le récepteur !")
}
```

*Existe aussi en client Python : `pip install ksbus` pour interconnecter vos workers d'IA ou scripts de scraping en temps réel.*

---

## 11. Serveur & Client Email Complet (ksmtp)

Oubliez les dépendances à des services SMTP tiers coûteux. Le package **`ksmtp`** (inclus dans `ksmux`) offre une stack email complète :

* **Serveur SMTP entrant/sortant** : Réception, envoi et relay intelligent avec résolution DNS MX automatique.
* **Signature DKIM native** : Authentification cryptographique de vos emails pour garantir une délivrabilité maximale sans spam.
* **Serveurs IMAP & POP3** intégrés (avec support TLS natif).
* **Templates d'emails** : Version HTML et texte brut avec injection de variables dynamiques.
* **Filtre Anti-Spam** : Blacklists IP, rate limiting et statistiques d'envoi.

```go
// Exemple d'envoi de template via ksmtp
server.SendTemplate("welcome", "admin@kamalshkeir.dev", "user@gmail.com", map[string]any{
    "Name": "Alexandre",
})
```

---

## 12. Reverse Proxy & Load Balancer Intégrés

Pourquoi installer et configurer Nginx, HAProxy ou Traefik pour répartir la charge ou exposer d'autres microservices ? **`ksmux` intègre nativement un Reverse Proxy et un Load Balancer avec Circuit Breaker**.

### A. Reverse Proxy (Sous-domaines ou URLs cibles)
Redirigez un sous-domaine complet vers un autre service interne ou externe en une ligne :

```go
app := srv.App()

// Redirige api.monsite.com vers un microservice interne
app.ReverseProxy("api.monsite.com", "http://127.0.0.1:8080")

// Redirige analytics.monsite.com vers un conteneur ou serveur externe
app.ReverseProxy("analytics.monsite.com", "http://10.0.0.5:9000")
```

### B. Load Balancer avec Circuit Breaker
Distribuez les requêtes entrantes entre plusieurs backends avec bascule automatique et protection en cas de panne :

```go
err := app.LoadBalancer("/api/v1/*",
    ksmux.BackendOpt{Url: "http://10.0.0.1:8080"},
    ksmux.BackendOpt{Url: "http://10.0.0.2:8080"},
    ksmux.BackendOpt{Url: "http://10.0.0.3:8080"},
)
lg.CheckError(err)
```
*Le Load Balancer distribue les requêtes, surveille l'état des serveurs et isole automatiquement les backends défaillants.*

---

## 13. Déploiement VPS : 1 Commande, Auto-HTTPS Let's Encrypt

Fini les configurations Nginx interminables, les renouvellements de certbot en cron et les crashs de reverse proxies.

### Le Code Go (dans votre `main.go`) :
```go
app.Config.Domain = "kamalshkeir.dev"
app.Config.SubDomains = []string{"www.kamalshkeir.dev"}
srv.RunAutoTLS()
```

### Le Déploiement :
1. **Compilez pour Linux** :
   ```sh
   GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o myapp .
   ```
2. **Envoyez sur le VPS** :
   ```sh
   scp myapp user@mon-vps.com:/opt/myapp/
   ```
3. **Redémarrez le service systemd** :
   ```sh
   ssh user@mon-vps.com "systemctl restart myapp"
   ```

### 🎯 Qu'est-ce qui se passe au démarrage ?
1. Le binaire s'ouvre sur les ports **80** et **443**.
2. Il contacte l'autorité Let's Encrypt, valide le challenge ACME et télécharge le certificat SSL.
3. Il active le serveur HTTPS avec HTTP/2 et force la redirection automatique du port 80 vers le 443.
4. Il démarre l'ORM KORM, applique les migrations, démarre le Dashboard Admin sur `/admin` et active le bus temps réel.
5. Il gère le renouvellement des certificats automatiquement en arrière-plan tous les 60 jours.

**Un seul binaire. 0 maintenance. 100% de disponibilité.**
