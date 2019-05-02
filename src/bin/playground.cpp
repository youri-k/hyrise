#include <iostream>

#include "types.hpp"

using namespace opossum;  // NOLINT

struct ExpressionOp {
  AbstractExpression e;
  

};

void evaluate_binary_predicate(const LeftOperands& as, const RightOperands& bs, const Range& range, Output& output) {
  Functor f;

  for (auto i = range.begin, i < range.end, ++i) {
    const auto& a = as[i];
    const auto& b = bs[i];

    if (a.is_null() || b.is_null()) {
      output.set(i, 0);
    } else {
      output.set(i, f(a.value(), b.value()));
    }
  }
}

void evaluate_arithmetics(const LeftOperands& as, const RightOperands& bs, const Range& range, Output& output) {
  Functor f;

  for (auto i = range.begin, j = 0, i < range.end, ++i, ++j) {
    const auto& a = as[i];
    const auto& b = bs[i];

    if (a.is_null() || b.is_null()) {
      output.set(j, 0);
    } else {
      output.set(j, f(a.value(), b.value()));
    }
  }
}

void evaluate_to_pos_list(const AbstractExpression& e, const Chunk& c) {
  const auto [plan, blocksize] = create_expression_plan(e);

  for (auto b = 0; b < c.size(); b += blocksize) {
    const auto e = b + blocksize;

    for (const auto& op : plan) {
      execute_op(op, Range{b, e});
    }
  }


  for (const auto& op : p) {

  }

}

int main() {
  std::cout << "Hello world!!" << std::endl;
  return 0;
}
