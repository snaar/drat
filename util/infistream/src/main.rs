extern crate actix;
extern crate actix_web;
extern crate env_logger;
extern crate flate2;
extern crate futures;
extern crate bytes;

use actix_web::{
    middleware, server, App, Body, Error, HttpRequest, HttpResponse,
};
use futures::Future;
use futures::future::result;

mod inficsv;
mod inficsvgz;
use inficsv::InfiCSV;
use inficsvgz::InfiCSVGZ;

fn index(_req: &HttpRequest) -> Box<Future<Item = HttpResponse, Error = Error>> {
    Box::new(result(Ok(HttpResponse::Ok().content_type("text/html").body(
        r#"<html><body><head></head>
        <p><a href="infinite.csv">infinite.csv</a></p>
        <p><a href="infinite.csv.gz">infinite.csv.gz</a></p>
        </body></html>"#,
    ))))
}

fn infinite_csv(_req: &HttpRequest) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let csv_body = Body::Streaming(Box::new(InfiCSV::new()));
    Box::new(result(Ok(HttpResponse::Ok().content_type("text/csv").body(csv_body))))
}

fn infinite_csv_gz(_req: &HttpRequest) -> Box<Future<Item = HttpResponse, Error = Error>> {
    let gz_body = Body::Streaming(Box::new(InfiCSVGZ::new()));
    Box::new(result(Ok(HttpResponse::Ok().content_type("application/gz").body(gz_body))))
}

fn main() {
    ::std::env::set_var("RUST_LOG", "actix_web=info");
    env_logger::init();
    let sys = actix::System::new("infistream");

    server::new(|| {
        App::new()
            .middleware(middleware::Logger::default())
            .resource("/infinite.csv", |r| r.f(infinite_csv))
            .resource("/infinite.csv.gz", |r| r.f(infinite_csv_gz))
            .resource("/", |r| r.f(index))
    }).workers(1)
        .bind("127.0.0.1:8182")
        .unwrap()
        .start();

    let _ = sys.run();
}
