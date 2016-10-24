name := "Spiko"

version := "0.0.2"

organization := "net.atos"

scalaVersion := "2.11.4"

//fork in Test := true

fork in run := true

test in assembly := {}

licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

libraryDependencies ++= Seq(
"com.typesafe.akka" %% "akka-actor" % "2.3.6" % "compile",
"com.tapad.scaerospike" %% "scaerospike" % "1.2.3",
"com.codahale.metrics" % "metrics-core" % "3.0.2",
"com.github.scopt" %% "scopt" % "3.2.0"
 )
 
 mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if x.contains(".properties") => MergeStrategy.last
    case x if Assembly.isConfigFile(x) =>
      MergeStrategy.concat
    case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
      MergeStrategy.rename
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.deduplicate
      } 
    case _ => MergeStrategy.deduplicate
  }
}

 resolvers ++= Seq(
 	"Maven Central" at "http://central.maven.org/maven2",
 	"Akka Repository" at "http://repo.akka.io/releases/",
		"Thrift" at "http://people.apache.org/~rawson/repo/",
			"Apache HBase" at "https://repository.apache.org/content/repositories/releases",
				"Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
					"krasserm at bintray" at "http://dl.bintray.com/krasserm/maven", 
						"kazan snapshots" at "http://kazan.priv.atos.fr/nexus/content/repositories/snapshots/",
						"Bintray Worldline" at "http://dl.bintray.com/worldline-messaging-org/maven/")

publishTo := Some("Bintray API Realm" at "https://api.bintray.com/maven/worldline-messaging-org/maven/spiko")
//credentials += Credentials("Bintray API Realm", "api.bintray.com", "giena", "b5a8c07925f14b3b24f557ac895ecba43a725a11")
credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
