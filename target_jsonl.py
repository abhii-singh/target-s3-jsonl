#!/usr/bin/env python3

import argparse
import io
import jsonschema
import simplejson as json
import os
import sys
from datetime import datetime
from pathlib import Path
import boto3

import singer
from jsonschema import Draft4Validator, FormatChecker
from adjust_precision_for_schema import adjust_decimal_precision_for_schema

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()



def persist_messages(
    messages,
    destination_path,
    custom_name=None,
    do_timestamp_file=True,
    write_to_s3=True,
    s3_bucket=None,
    s3_prefix=None,
):
    state = None
    schemas = {}
    key_properties = {}
    validators = {}

    timestamp_file_part = '-' + datetime.now().strftime('%Y%m%dT%H%M%S') if do_timestamp_file else ''

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception(
                    "A record for stream {}"
                    "was encountered before a corresponding schema".format(o['stream'])
                )

            try: 
                validators[o['stream']].validate((o['record']))
            except jsonschema.ValidationError as e:
                logger.error(f"Failed parsing the json schema for stream: {o['stream']}.")
                raise e

            filename = (custom_name or o['stream']) + timestamp_file_part + '.jsonl'

            if write_to_s3:
                if not s3_bucket:
                    raise Exception(f"Value {s3_bucket} must be provided because the write_to_s3 flag is set to True")
                if not s3_prefix:
                    raise Exception(f"Value {s3_prefix} must be provided because the write_to_s3 flag is set to True")
                
                s3 = boto3.resource('s3')
                s3object = s3.Object(s3_bucket, f'{s3_prefix}{filename}')
                json_d = json.dumps(o['record']) + '\n'
                s3object.put(
                    Body=(bytes(json_d.encode('UTF-8')))
                    )
            else:
                if destination_path:
                    Path(destination_path).mkdir(parents=True, exist_ok=True)
                filename = os.path.expanduser(os.path.join(destination_path, filename))

                with open(filename, 'a', encoding='utf-8') as json_file:
                    json_file.write(json.dumps(o['record']) + '\n')

                state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            adjust_decimal_precision_for_schema(schemas[stream])
            validators[stream] = Draft4Validator((o['schema']))
            key_properties[stream] = o['key_properties']
        else:
            logger.warning("Unknown message type {} in message {}".format(o['type'], o))

    return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(
        input_messages,
        config.get('destination_path', ''),
        config.get('custom_name', ''),
        config.get('do_timestamp_file', True),
        config.get('write_to_s3', True),
        config.get('s3_bucket', ''),
        config.get('s3_prefix', '')
    )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
