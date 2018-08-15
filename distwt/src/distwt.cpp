#include <thrill/api/dia.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>

//! A 2-dimensional point with double precision
struct Point {
    //! point coordinates
    double x, y;
};

//! make ostream-able for Print()
std::ostream& operator << (std::ostream& os, const Point& p) {
    return os << '(' << p.x << ',' << p.y << ')';
}

void Process(thrill::Context& ctx) {
    std::default_random_engine rng(std::random_device { } ());
    std::uniform_real_distribution<double> dist(0.0, 1000.0);
    // generate 100 random points using uniform distribution
    thrill::DIA<Point> points =
        thrill::api::Generate(
            ctx, /* size */ 100,
            [&](const size_t& /* index */) {
                return Point { dist(rng), dist(rng) };
            })
        .Cache();
    // print out the points
    points.Print("points");
}

int main(int argc, const char** argv) {
    // launch Thrill program: the lambda function will be run on each worker.
    return thrill::Run(
        [&](thrill::Context& ctx) { Process(ctx); });
}

