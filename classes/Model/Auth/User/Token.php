<?php
/**
 * Default auth user toke
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\ORM\Model\Auth\User;

use Modseven\ORM\ORM;
use Modseven\Validation;
use Modseven\Database\DB;
use Modseven\ORM\Exception;
use Modseven\ORM\Model\Auth\User;

class Token extends ORM
{

    protected string $_table_name = 'user_tokens';

    /**
     * Belongs to
     * @var array
     */
    protected array $_belongs_to = [
        'user' => ['model' => User::class, 'foreign_key' => 'user_id'],
    ];

    /**
     * Column created
     * @var mixed
     */
    protected $_created_column = [
        'column' => 'created',
        'format' => true,
    ];

    /**
     * Handles garbage collection and deleting of expired objects.
     *
     * @param mixed $id ID
     *
     * @throws Exception;
     * @throws \Exception;
     * @throws \Modseven\Exception
     * @throws \Modseven\Database\Exception
     */
    public function __construct($id = null)
    {
        parent::__construct($id);

        if (random_int(1, 100) === 1) {
            // Do garbage collection
            $this->deleteExpired();
        }

        if ($this->expires < time() && $this->_loaded) {
            // This object has expired
            $this->delete();
        }
    }

    /**
     * Deletes all expired tokens.
     *
     * @return  self
     *
     * @throws Exception
     */
    public function deleteExpired() : self
    {
        // Delete all expired tokens
        try
        {
            DB::delete($this->_table_name)
                                 ->where('expires', '<', time())
                                 ->execute($this->_db);
        }
        catch (\Modseven\Exception $e)
        {
            throw new Exception($e->getMessage(), null, $e->getCode(), $e);
        }


        return $this;
    }

    /**
     * @param Validation|null $validation
     *
     * @return ORM
     * @throws \Exception
     * @throws \Modseven\ORM\Validation\Exception
     */
    public function create(?Validation $validation = null) : ORM
    {
        $this->token = $this->createToken();

        return parent::create($validation);
    }

    /**
     * Create auth token
     *
     * @return string
     *
     * @throws \Exception
     */
    protected function createToken() : string
    {
        do
        {
            $token = sha1(uniqid(\Modseven\Text::random('alnum', 32), true));
        } while (ORM::factory(self::class, ['token' => $token])->loaded());

        return $token;
    }
}