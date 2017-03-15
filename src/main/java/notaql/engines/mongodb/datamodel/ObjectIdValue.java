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

package notaql.engines.mongodb.datamodel;

import org.bson.types.ObjectId;

import notaql.datamodel.ComplexValue;
import notaql.datamodel.GenericValue;
import notaql.datamodel.Value;

/**
 * Represents an ObjectId in Mongo
 */
public class ObjectIdValue extends GenericValue<ObjectId> {
    private static final long serialVersionUID = -4550114968771440973L;

    public ObjectIdValue(ObjectId value) {
        super(value);
    }

    public ObjectIdValue(ObjectId value, ComplexValue<?> parent) {
        super(value, parent);
    }

    @Override
    public byte[] toBytes() {
        return getValue().toByteArray();
    }

    @Override
    public Value deepCopy() {
        return new ObjectIdValue(getValue());
    }
}
