/*
 * Copyright 2015 by Thomas Lottermann
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package notaql.engines.hbase.parser.path;

import org.antlr.v4.runtime.misc.NotNull;

import notaql.datamodel.Step;
import notaql.engines.EngineEvaluator;
import notaql.engines.hbase.HBaseApi;
import notaql.model.path.IdStep;
import notaql.model.path.IgnoredIdStep;
import notaql.model.path.OutputPath;
import notaql.model.path.OutputPathStep;
import notaql.model.path.ResolvedIdStep;
import notaql.parser.TransformationParser;
import notaql.parser.antlr.NotaQL2ColumnOutBaseVisitor;
import notaql.parser.antlr.NotaQL2ColumnOutParser;

/**
 * Extracts an OutputPath from given parse tree.
 */
public class HBaseOutputPathVisitor extends NotaQL2ColumnOutBaseVisitor<OutputPath> {
    private final TransformationParser parser;

    public HBaseOutputPathVisitor(TransformationParser parser, boolean relative) {
        this.parser = parser;
    }

    /**
     * Here we parse the output row id (OUT._r)
     * @param ctx
     * @return
     */
    @Override
    public OutputPath visitRowOutputPath(@NotNull NotaQL2ColumnOutParser.RowOutputPathContext ctx) {
        return new OutputPath(new IdStep<>(EngineEvaluator.getRowIdentifierStep()));
    }

    /**
     * Here we parse constant output cells (e.g. OUT.a)
     * @param ctx
     * @return
     */
    @Override
    public OutputPath visitColIdOutputPath(@NotNull NotaQL2ColumnOutParser.ColIdOutputPathContext ctx) {
        final OutputPathStep familyStep =
                ctx.colId().colFamily==null ?
                        new IdStep<>(new Step<>(HBaseApi.DEFAULT_COLUMN_FAMILY))
                        : new IdStep<>(new Step<>(ctx.colId().colFamily.getText()));
        final IdStep<String> step;

        if(ctx.colId().colName.getText().equals("_"))
            step = new IgnoredIdStep();
        else
            step = new IdStep<>(new Step<>(ctx.colId().colName.getText()));

        return new OutputPath(
                familyStep,
                step
        );
    }

    /**
     * Here we parse resolved output path cells (e.g. OUT.$(IN.a))
     * @param ctx
     * @return
     */
    @Override
    public OutputPath visitResolvedOutputPath(@NotNull NotaQL2ColumnOutParser.ResolvedOutputPathContext ctx) {
        final OutputPathStep familyStep =
                ctx.colFamily==null ?
                        new IdStep<>(new Step<>(HBaseApi.DEFAULT_COLUMN_FAMILY))
                        : new IdStep<>(new Step<>(ctx.colFamily.getText()));

        return new OutputPath(
                familyStep,
                new ResolvedIdStep(parser.getInputPathVisitor().evaluateAbsolute(ctx.absoluteInputPath().path().getText()))
        );
    }

    /**
     * Here we parse a completely resolved path (e.g. OUT.$(IN.f):$(IN.a))
     * @param ctx
     * @return
     */
    @Override
    public OutputPath visitResolvedOutputPathFamily(@NotNull NotaQL2ColumnOutParser.ResolvedOutputPathFamilyContext ctx) {
        return new OutputPath(
                new ResolvedIdStep(parser.getInputPathVisitor().evaluateAbsolute(ctx.family.path().getText())),
                new ResolvedIdStep(parser.getInputPathVisitor().evaluateAbsolute(ctx.column.path().getText()))
        );
    }
}
