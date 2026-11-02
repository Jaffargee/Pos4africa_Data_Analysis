
class POSRejectedError(Exception):
      """POS answered but refused to create the customer. Retrying won't help."""

class WritebackError(Exception):
      """Customer exists in POS but pos_customer_id could not be saved in Supabase."""

      def __init__(self, sb_id, pos_person_id, reason: str) -> None:
            super().__init__(f"sb_id={sb_id} pos_person_id={pos_person_id}: {reason}")
            self.sb_id = sb_id
            self.pos_person_id = pos_person_id
