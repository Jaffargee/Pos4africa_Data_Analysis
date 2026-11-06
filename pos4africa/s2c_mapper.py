import argparse
import asyncio
import logging
import sys

from pos4africa.worker.pipeline.base import PipelineContext
from pos4africa.worker.pipeline.s2c_mapper import S2CMapper

log = logging.getLogger("cli")


def build_parser() -> argparse.ArgumentParser:
      ap = argparse.ArgumentParser(
            description="Run the S2CMapper pipeline. Dry-run unless --commit is given.",
            epilog="Examples:\n"
                  "  --comment 'SAMILA SANI' --new-comment 'SAMIHA SANI'            (dry run)\n"
                  "  --comment 'SAMILA SANI' --new-comment 'SAMIHA SANI' --commit   (writes)\n"
                  "  --run-mapper",
            formatter_class=argparse.RawDescriptionHelpFormatter,
      )
      ap.add_argument("--comment", help="Existing comment to rename (requires --new-comment).")
      ap.add_argument("--new-comment", help="Replacement comment (requires --comment).")
      ap.add_argument("--run-mapper", action="store_true", help="Run the full comment-to-customer mapper.")
      ap.add_argument("--commit", action="store_true", help="Apply changes. Without it, nothing is written.")
      return ap


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
      ap = build_parser()
      args = ap.parse_args(argv)

      # 1. normalise: whitespace-only counts as "not given"
      args.comment = (args.comment or "").strip() or None
      args.new_comment = (args.new_comment or "").strip() or None

      # 2. both-or-neither
      if bool(args.comment) != bool(args.new_comment):
            given, missing = ("--comment", "--new-comment") if args.comment else ("--new-comment", "--comment")
            ap.error(f"{given} was given without {missing}; they must be used together.")

      # 3. the pair must change something
      if args.comment and args.comment == args.new_comment:
            ap.error("--comment and --new-comment are identical; nothing to change.")

      # 4. at least one action
      rename = bool(args.comment and args.new_comment)
      if not rename and not args.run_mapper:
            ap.error("nothing to do: give --comment with --new-comment, or --run-mapper.")

      # 5. --commit alone is meaningless
      if args.commit and not (rename or args.run_mapper):
            ap.error("--commit needs an action to apply.")

      args.rename = rename
      return args


async def run(args: argparse.Namespace) -> int:
      ctx = PipelineContext()
      mapper = S2CMapper()
      mode = "COMMIT" if args.commit else "DRY-RUN"
      log.info("mode=%s rename=%s run_mapper=%s", mode, args.rename, args.run_mapper)

      if args.rename:
            await mapper.mapper_editor(args.comment, args.new_comment, commit=args.commit)
      if args.run_mapper:
            await mapper.run(ctx)
      return 0


def main(argv: list[str] | None = None) -> int:
      logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
      args = parse_args(argv)          # exits with code 2 on bad input
      try:
            return asyncio.run(run(args))
      except KeyboardInterrupt:
            log.warning("interrupted")
            return 130
      except Exception:
            log.exception("failed")
            return 1


if __name__ == "__main__":
      sys.exit(main())