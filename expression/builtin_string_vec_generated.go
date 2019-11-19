// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by go generate in expression/generator; DO NOT EDIT.

package expression

import (
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// vecEvalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	arg0 := buf0.Int64s()

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = 0
	}
	for i := 1; i < len(b.args); i++ {
		if err := b.args[i].VecEvalInt(b.ctx, input, buf1); err != nil {
			return err
		}

		arg1 := buf1.Int64s()

		for j := 0; j < n; j++ {
			if i64s[j] > 0 || buf0.IsNull(j) || buf1.IsNull(j) {
				continue
			}

			if arg0[j] == arg1[j] {

				i64s[j] = int64(i)
			}
		}
	}
	return nil
}

func (b *builtinFieldIntSig) vectorized() bool {
	return true
}

// vecEvalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	arg0 := buf0.Float64s()

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = 0
	}
	for i := 1; i < len(b.args); i++ {
		if err := b.args[i].VecEvalReal(b.ctx, input, buf1); err != nil {
			return err
		}

		arg1 := buf1.Float64s()

		for j := 0; j < n; j++ {
			if i64s[j] > 0 || buf0.IsNull(j) || buf1.IsNull(j) {
				continue
			}

			if arg0[j] == arg1[j] {

				i64s[j] = int64(i)
			}
		}
	}
	return nil
}

func (b *builtinFieldRealSig) vectorized() bool {
	return true
}

// vecEvalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = 0
	}
	for i := 1; i < len(b.args); i++ {
		if err := b.args[i].VecEvalString(b.ctx, input, buf1); err != nil {
			return err
		}

		for j := 0; j < n; j++ {
			if i64s[j] > 0 || buf0.IsNull(j) || buf1.IsNull(j) {
				continue
			}

			if buf0.GetString(j) == buf1.GetString(j) {

				i64s[j] = int64(i)
			}
		}
	}
	return nil
}

func (b *builtinFieldStringSig) vectorized() bool {
	return true
}
