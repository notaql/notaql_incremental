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

package notaql.model.vdata;

/**
 * Created by thomas on 22.03.15.
 */
public class GenericFunctionVData implements  VData {
	private static final long serialVersionUID = -6334084825822052381L;
	
	private final String name;
    private final VData[] args;

    public GenericFunctionVData(String name, VData... args) {
        this.name = name;
        this.args = args;
    }

    public String getName() {
        return name;
    }

    public VData[] getArgs() {
        return args;
    }
}
