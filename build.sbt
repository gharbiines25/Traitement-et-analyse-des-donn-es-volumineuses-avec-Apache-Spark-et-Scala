name := "EcommerceSparkProject"

version := "0.1"

scalaVersion := "2.13.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
  "org.plotly-scala" %% "plotly-render" % "0.8.2"

)


lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-spark-project3"
  )
