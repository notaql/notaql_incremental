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

package notaql.parser;

import org.antlr.v4.runtime.misc.NotNull;

import notaql.datamodel.AtomValue;
import notaql.datamodel.BooleanValue;
import notaql.datamodel.NumberValue;
import notaql.datamodel.StringValue;
import notaql.parser.antlr.NotaQL2BaseVisitor;
import notaql.parser.antlr.NotaQL2Parser;

/**
 * Created by Thomas Lottermann on 07.12.14.
 */
public class AtomValueVisitor extends NotaQL2BaseVisitor<AtomValue<?>> {
    public AtomValueVisitor(TransformationParser transformationParser) {}

    @Override
    public NumberValue visitNumberAtom(@NotNull NotaQL2Parser.NumberAtomContext ctx) {
        String string;
        if(ctx.Float() != null) {
            string = ctx.Float().getText();
        } else {
            string = ctx.Int().getText();
        }

        if(ctx.Float() != null) {
            return new NumberValue(Double.parseDouble(string));
        }
        return new NumberValue(Integer.parseInt(string));
    }

    @Override
    public StringValue visitStringAtom(@NotNull NotaQL2Parser.StringAtomContext ctx) {
        String string = ctx.String().getText().replace("\\'", "'");
        string = string.substring(1, string.length() - 1);
        return new StringValue(string);
    }

    @Override
    public AtomValue<?> visitFalseAtom(@NotNull NotaQL2Parser.FalseAtomContext ctx) {
        return new BooleanValue(false);
    }

    @Override
    public AtomValue<?> visitTrueAtom(@NotNull NotaQL2Parser.TrueAtomContext ctx) {
        return new BooleanValue(true);
    }
}
