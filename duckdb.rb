require "js"

JS_TRUE = JS.eval("return true;")

JS.eval("this.ruby_context = {};")
JS.eval(<<-JAVA_SCRIPT).await
  return (async () => {
    ruby_context.duckdb = await import("https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.20.0/+esm");
  })();
JAVA_SCRIPT
ruby_context = JS.global[:ruby_context]
duckdb = ruby_context[:duckdb]
arrow = ruby_context[:arrow]
bundles = duckdb.getJsDelivrBundles
bundle = duckdb.selectBundle(bundles).await
ruby_context[:bundle] = bundle
JS.eval(<<-JAVA_SCRIPT).await
  return (async () => {
    const duckdb = ruby_context.duckdb;
    const bundle = ruby_context.bundle;
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`],
               {type: "text/javascript"})
    );
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    ruby_context.db = new duckdb.AsyncDuckDB(logger, worker);
    await ruby_context.db.instantiate(bundle.mainModule, bundle.pthredWorker);
    URL.revokeObjectURL(worker_url);
  })();
JAVA_SCRIPT
db = ruby_context[:db]
connection = db.connect().await
response = JS.global.fetch("data.arrows").await
reader = response[:body].getReader
options = JS.eval("return {\"name\": \"data\"};")
inserts = JS.eval("return [];")
loop do
  result = reader.read.await
  break if result[:done] == JS_TRUE
  inserts.push(connection.insertArrowFromIPCStream(result[:value], options))
end
JS.global[:Promise].all(inserts).await
p connection.query("SELECT * FROM data").await
