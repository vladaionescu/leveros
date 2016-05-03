
import 'source-map-support/register';

import * as client from './client';
import lodash from 'lodash';

const exported = lodash.extend({}, client);
module.exports = exported;
